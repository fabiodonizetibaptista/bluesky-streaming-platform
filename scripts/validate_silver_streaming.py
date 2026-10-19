#!/usr/bin/env python3
"""Validação ponta a ponta Kafka -> Flink -> Silver Streaming.

Uso:
    .venv/bin/python scripts/validate_silver_streaming.py --run

Este teste publica cinco eventos sintéticos no Kafka existente.
Exige --run para impedir publicação acidental.

Não elimina volumes, não interrompe containers e não remove saídas.
Em caso de falha após a submissão, preserva o job para diagnóstico.
"""

import argparse
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from confluent_kafka import Consumer, Producer, TopicPartition

SQL_TEMPLATE = Path("sql/06_kafka_to_silver_streaming.sql")
TOPIC = "bluesky.posts.raw"
FLINK_API = "http://127.0.0.1:8081"

FLINK_CLIENT = [
    "docker", "compose", "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]

ERROR_MARKERS = (
    "ValidationException",
    "SqlParserException",
    "SQL execution error",
    "SQL parse failed",
    "SQL validation failed",
    "[ERROR]",
)

ACTIVE_STATES = {
    "CREATED", "RUNNING", "RESTARTING",
    "RECONCILING", "FAILING", "CANCELLING",
    "INITIALIZING", "SUSPENDED",
}


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def api(endpoint):
    """Consulta Flink REST com retry apenas para falhas transitórias."""
    for attempt in range(3):
        try:
            with urlopen(FLINK_API + endpoint, timeout=10) as response:
                return json.load(response)

        except HTTPError as exc:
            # Erros 4xx definitivos não devem ser escondidos.
            # 429 e erros 5xx podem ser transitórios.
            if exc.code != 429 and exc.code < 500:
                raise RuntimeError(
                    f"FLINK_REST_HTTP_{exc.code}"
                ) from exc
            if attempt == 2:
                raise RuntimeError(
                    f"FLINK_REST_HTTP_RETRIES_EXHAUSTED_{exc.code}"
                ) from exc

        except (URLError, TimeoutError, ConnectionError) as exc:
            if attempt == 2:
                raise RuntimeError(
                    "FLINK_REST_TRANSPORT_RETRIES_EXHAUSTED"
                ) from exc

        time.sleep(min(2 ** attempt, 4))

    raise RuntimeError("FLINK_REST_UNEXPECTED_RETRY_EXIT")


def position(row):
    """Aplica o contrato de coordenadas Kafka da Silver bounded."""
    require(isinstance(row, dict), "INVALID_KAFKA_RECORD")

    for field in ("kafka_partition", "kafka_offset"):
        value = row.get(field)
        require(
            type(value) is int and value >= 0,
            f"INVALID_KAFKA_POSITION: {field}",
        )

    return row["kafka_partition"], row["kafka_offset"]


def finalized_rows(directory):
    """Ignora arquivos não finalizados pelo filesystem sink."""
    rows = []

    if not directory.is_dir():
        return rows

    for file in sorted(directory.rglob("*")):
        if not file.is_file():
            continue

        if (
            file.name.startswith(".")
            or ".inprogress" in file.name
            or ".pending" in file.name
        ):
            continue

        with file.open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    rows.append(json.loads(line))

    return rows


def unfinished_file_count(directory):
    """Conta arquivos ainda não finalizados, sem ler seu conteúdo."""
    if not directory.is_dir():
        return 0

    return sum(
        1
        for file in directory.rglob("*")
        if file.is_file()
        and (
            file.name.startswith(".")
            or ".inprogress" in file.name
            or ".pending" in file.name
        )
    )


def execute_sql(sql, *, explain=False):
    """Executa o SQL Client sem imprimir SQL ou logs integrais."""
    if explain:
        sql, changed = re.subn(
            r"(?m)^EXECUTE STATEMENT SET\s*$",
            "EXPLAIN EXECUTE STATEMENT SET",
            sql,
            count=1,
        )
        require(changed == 1, "STATEMENT_SET_NOT_FOUND")

    try:
        result = subprocess.run(
            FLINK_CLIENT,
            input=sql,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except subprocess.TimeoutExpired as exc:
        # O SQL Client pode ter submetido o job antes do timeout.
        # Evita orientar uma nova execução que criaria job concorrente.
        raise RuntimeError(
            "FLINK_SQL_CLIENT_TIMEOUT_SUBMISSION_UNKNOWN_"
            "INSPECT_FLINK_REST_BEFORE_RETRY"
        ) from exc

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    detected = [
        marker for marker in ERROR_MARKERS
        if marker.lower() in output.lower()
    ]

    print("SQL_EXIT_CODE =", result.returncode)
    print("SQL_ERROR_TYPES =", detected)

    require(
        result.returncode == 0 and not detected,
        "FLINK_SQL_FAILED_INSPECT_JOB_BEFORE_RETRY",
    )

    if explain:
        require("Physical Plan" in output, "FLINK_PLAN_MISSING")
        print("FLINK_EXPLAIN = PASSED")
        return None

    ids = list(dict.fromkeys(
        re.findall(r"\b[0-9a-f]{32}\b", output)
    ))
    require(ids, "JOB_ID_NOT_PARSED_INSPECT_BEFORE_RETRY")
    return ids[-1]


def kafka_watermarks():
    """Consulta offsets de todas as partições sem consumir eventos."""
    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": f"silver-validator-preflight-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
    })

    try:
        metadata = consumer.list_topics(TOPIC, timeout=10)
        topic = metadata.topics.get(TOPIC)

        require(topic is not None, "KAFKA_TOPIC_NOT_FOUND")
        require(not topic.error, "KAFKA_TOPIC_METADATA_ERROR")
        require(topic.partitions, "KAFKA_TOPIC_HAS_NO_PARTITIONS")

        result = {}

        for partition_id, partition in sorted(topic.partitions.items()):
            require(
                not partition.error,
                "KAFKA_PARTITION_METADATA_ERROR",
            )

            result[partition_id] = consumer.get_watermark_offsets(
                TopicPartition(TOPIC, partition_id),
                timeout=10,
                cached=False,
            )

        return result
    finally:
        consumer.close()


def make_sql(run_id):
    """Isola grupo Kafka e destinos; mantém as regras de negócio."""
    sql = SQL_TEMPLATE.read_text(encoding="utf-8")

    replacements = {
        "flink-bluesky-silver-streaming-v1":
            f"flink-bluesky-silver-validation-{run_id}",
        "'scan.startup.mode' = 'earliest-offset'":
            "'scan.startup.mode' = 'latest-offset'",
        "/silver/streaming_valid_v1":
            f"/silver/validation_runs/{run_id}/valid",
        "/silver/streaming_quarantine_v1":
            f"/silver/validation_runs/{run_id}/quarantine",
    }

    for original, replacement in replacements.items():
        require(
            sql.count(original) == 1,
            f"SQL_CONTRACT_CHANGED: {original}",
        )
        sql = sql.replace(original, replacement, 1)

    require(
        sql.count("'sink.rolling-policy.rollover-interval'") == 2,
        "ROLLING_POLICY_NOT_CONFIGURED",
    )
    require(
        sql.count("'sink.rolling-policy.check-interval'") == 2,
        "ROLLING_CHECK_NOT_CONFIGURED",
    )
    require(
        "SET 'execution.checkpointing.interval' = '10s';" in sql,
        "CHECKPOINT_NOT_CONFIGURED",
    )

    return sql


def make_cases():
    """Cinco cenários controlados, conforme contrato Silver V1."""
    seq = int(time.time() * 1_000_000)

    def event(index, **overrides):
        payload = {
            "source": "bluesky_jetstream_v2",
            "source_seq": seq + index,
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "event_time": "2026-10-18T12:00:00.123456Z",
            "witnessed_at": "2026-10-18T12:00:01.123456Z",
        }
        payload.update(overrides)
        return payload

    return [
        ("VALID_MICROSECONDS", event(0), "VALID"),
        ("VALID_SECONDS", event(
            1,
            event_time="2026-10-18T12:00:00Z",
            witnessed_at="2026-10-18T12:00:01Z",
        ), "VALID"),
        ("INVALID_OPERATION", event(
            2, operation="unknown",
        ), "INVALID_OPERATION"),
        ("INVALID_SEQUENCE", event(
            3, source_seq="123",
        ), "INVALID_SEQUENCE"),
        ("UNSUPPORTED_SOURCE", event(
            4, source="synthetic_test_source",
        ), "UNSUPPORTED_SOURCE"),
    ]


def publish(cases):
    """Confirma cada publicação e devolve suas posições reais."""
    producer = Producer({
        "bootstrap.servers": "127.0.0.1:9092",
        "enable.idempotence": True,
        "acks": "all",
    })

    delivered = {}
    failures = []

    for label, payload, _ in cases:

        def callback(error, message, label=label):
            if error:
                failures.append(label)
            else:
                delivered[label] = (
                    message.partition(),
                    message.offset(),
                )

        producer.produce(
            TOPIC,
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=callback,
        )

    pending = producer.flush(30)

    print("KAFKA_EVENTS_CONFIRMED =", len(delivered), flush=True)
    print("KAFKA_DELIVERY_FAILURES =", len(failures))

    return delivered, pending, failures


def write_json_exclusive(path, value):
    """Cria artefatos com permissões restritas, sem sobrescrever."""
    fd = os.open(
        str(path),
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        0o600,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        action="store_true",
        help="Autoriza submissão Flink e publicação de 5 eventos Kafka.",
    )
    args = parser.parse_args()

    if not args.run:
        parser.print_help()
        return

    run_id = uuid.uuid4().hex[:12]
    root = (
        Path("runtime/flink/silver/validation_runs") / run_id
    )
    metadata_path = Path("/tmp") / f"silver-validation-{run_id}.json"
    receipt_path = Path("/tmp") / f"silver-validation-{run_id}-receipt.json"

    print("=== SILVER STREAMING VALIDATION ===", flush=True)
    print("RUN_ID =", run_id, flush=True)

    require(SQL_TEMPLATE.is_file(), "SQL_TEMPLATE_MISSING")
    require(not root.exists(), "RUN_DIRECTORY_ALREADY_EXISTS")

    active_jobs = [
        job for job in api("/jobs/overview").get("jobs", [])
        if job.get("state") in ACTIVE_STATES
    ]
    require(not active_jobs, "ACTIVE_FLINK_JOB_PRESENT")

    overview = api("/overview")
    require(
        overview.get("slots-available", 0) >= 1,
        "NO_AVAILABLE_FLINK_SLOT",
    )

    watermarks = kafka_watermarks()
    print("KAFKA_PARTITIONS =", len(watermarks))

    for partition_id, (low, high) in sorted(watermarks.items()):
        print("KAFKA_PARTITION =", partition_id)
        print("KAFKA_EARLIEST_OFFSET =", low)
        print("KAFKA_NEXT_OFFSET =", high)

    sql = make_sql(run_id)
    execute_sql(sql, explain=True)

    job_id = execute_sql(sql)
    print("JOB_ID =", job_id, flush=True)

    # Persiste a identidade imediatamente após a submissão.
    write_json_exclusive(metadata_path, {
        "run_id": run_id,
        "job_id": job_id,
        "output_directory": str(root),
        "topic": TOPIC,
    })

    print("RUN_METADATA =", metadata_path, flush=True)

    # Aguarda checkpoint inicial: o consumidor Kafka já deve estar
    # executando antes de publicarmos no modo latest-offset.
    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        state = api(f"/jobs/{job_id}").get("state")
        require(
            state not in {"FAILED", "FINISHED", "CANCELED"},
            f"JOB_TERMINATED: {state}",
        )

        completed = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if state == "RUNNING" and completed >= 1:
            break

        time.sleep(3)
    else:
        raise RuntimeError("JOB_NOT_READY_DO_NOT_RESUBMIT")

    print("INITIAL_CHECKPOINTS =", completed, flush=True)

    cases = make_cases()
    delivered, pending, failures = publish(cases)

    # Salva offsets confirmados ANTES da reconciliação.
    write_json_exclusive(receipt_path, {
        "run_id": run_id,
        "job_id": job_id,
        "positions": {
            label: list(value)
            for label, value in delivered.items()
        },
    })

    print("KAFKA_RECEIPT =", receipt_path, flush=True)

    require(
        pending == 0 and not failures and len(delivered) == 5,
        "KAFKA_DELIVERY_INCOMPLETE_DO_NOT_REPUBLISH",
    )

    expected = {
        delivered[label]: classification
        for label, _, classification in cases
    }

    deadline = time.monotonic() + 180
    observed = {}
    duplicates = 0
    completed_after = completed

    while time.monotonic() < deadline:
        state = api(f"/jobs/{job_id}").get("state")
        require(state == "RUNNING", f"JOB_NOT_RUNNING: {state}")

        observed = {}
        duplicates = 0

        for name in ("valid", "quarantine"):
            for row in finalized_rows(root / name):
                key = position(row)

                if key not in expected:
                    continue

                if key in observed:
                    duplicates += 1
                    continue

                observed[key] = (
                    "VALID" if name == "valid"
                    else row["rejection_reason"]
                )

        completed_after = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if (
            len(observed) == 5
            and duplicates == 0
            and completed_after > completed
        ):
            break

        time.sleep(5)

    print("=== RECONCILIATION ===")
    print("MATCHED_POSITIONS =", len(observed))
    print("DUPLICATE_POSITIONS =", duplicates)
    print("CHECKPOINTS_AFTER =", completed_after)

    for label, _, classification in cases:
        actual = observed.get(delivered[label])
        print(
            label, "=",
            "PASSED" if actual == classification else "FAILED",
        )

    passed = (
        observed == expected
        and duplicates == 0
        and completed_after > completed
        and api(f"/jobs/{job_id}").get("state") == "RUNNING"
    )

    print(
        "SILVER_STREAMING_VALIDATION =",
        "PASSED" if passed else "FAILED",
    )
    final_state = api(f"/jobs/{job_id}").get("state")
    print("FINAL_JOB_STATE =", final_state)
    print("JOB_LEFT_RUNNING =", final_state == "RUNNING")
    print(
        "VALID_UNFINISHED_FILES =",
        unfinished_file_count(root / "valid"),
    )
    print(
        "QUARANTINE_UNFINISHED_FILES =",
        unfinished_file_count(root / "quarantine"),
    )
    print("PREVIOUS_OUTPUTS_PRESERVED = True")
    print("DO_NOT_RERUN_AFTER_KAFKA_PUBLICATION = True")

    require(passed, "STREAMING_VALIDATION_FAILED_INSPECT_EXISTING_JOB")


if __name__ == "__main__":
    main()
