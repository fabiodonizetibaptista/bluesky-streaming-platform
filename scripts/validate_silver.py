"""Testes integrados Silver usando o Flink SQL real.

Cada execução cria diretórios novos, não modifica o SQL e
reconcilia os registros por (partição Kafka, offset).

Não imprime payloads nem identificadores de eventos.
"""

import json
import re
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path
from urllib.request import urlopen


ROOT = Path("runtime/flink")
SQL_PATH = Path("sql/05_bronze_to_silver.sql")

RUN_ID = uuid.uuid4().hex[:12]

RUN_ROOT = ROOT / "silver_validation" / RUN_ID
FIXTURE_SOURCE = RUN_ROOT / "fixtures"
FIXTURE_VALID = RUN_ROOT / "fixture_valid"
FIXTURE_QUARANTINE = RUN_ROOT / "fixture_quarantine"
HISTORICAL_VALID = RUN_ROOT / "historical_valid"
HISTORICAL_QUARANTINE = RUN_ROOT / "historical_quarantine"

FLINK_CMD = [
    "docker", "compose", "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]


def require(condition, error):
    if not condition:
        raise RuntimeError(error)


def read_rows(directory):
    require(directory.is_dir(), "OUTPUT_DIRECTORY_MISSING")

    rows = []
    for file in sorted(directory.rglob("*")):
        if not file.is_file():
            continue

        require(
            not file.name.startswith(".")
            and not file.name.endswith(".inprogress"),
            "UNFINALIZED_OUTPUT_FILE",
        )

        for line in file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))

    return rows


def position(row):
    return (
        int(row["kafka_partition"]),
        int(row["kafka_offset"]),
    )


def reconcile(source, valid, quarantine, label):
    source_positions = [position(row) for row in source]
    output_positions = [
        position(row) for row in valid + quarantine
    ]

    require(
        len(source_positions) == len(set(source_positions)),
        f"{label}_SOURCE_DUPLICATES",
    )
    require(
        len(output_positions) == len(set(output_positions)),
        f"{label}_OUTPUT_DUPLICATES",
    )
    require(
        set(source_positions) == set(output_positions),
        f"{label}_POSITION_MISMATCH",
    )

    print(f"{label}_INPUT =", len(source))
    print(f"{label}_VALID =", len(valid))
    print(f"{label}_QUARANTINE =", len(quarantine))
    print(f"{label}_RECONCILIATION = PASSED")


def adapt_sql(template, source, valid, quarantine):
    """Substitui exclusivamente os caminhos previstos no SQL."""
    replacements = {
        "/bronze/events_v1": source,
        "/silver/valid_v3": valid,
        "/silver/quarantine_v3": quarantine,
    }

    sql = template

    for old, new in replacements.items():
        require(sql.count(old) == 1, "SQL_PATH_CONTRACT_CHANGED")
        sql = sql.replace(old, new, 1)

    return sql


def submit(sql, label, explain=False):
    """Valida e executa sem imprimir o conteúdo dos registros."""
    if explain:
        sql, changed = re.subn(
            r"(?m)^EXECUTE STATEMENT SET\s*$",
            "EXPLAIN EXECUTE STATEMENT SET",
            sql,
            count=1,
        )
        require(changed == 1, "STATEMENT_SET_NOT_FOUND")

    result = subprocess.run(
        FLINK_CMD,
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
    )

    output = result.stdout

    # Guardamos o diagnóstico fora do repositório e não o
    # imprimimos integralmente, evitando exposição de payloads.
    log_path = Path(
        f"/tmp/flink-silver-{RUN_ID}-{label}.log"
    )
    log_path.write_text(output, encoding="utf-8")

    errors = (
        "ValidationException",
        "SqlParserException",
        "SQL execution error",
        "SQL parse failed",
        "SQL validation failed",
        "UnsupportedOperationException",
        "[ERROR]",
    )

    detected = [
        name for name in errors
        if name.lower() in output.lower()
    ]

    print(f"{label}_EXIT_CODE =", result.returncode)
    print(f"{label}_ERROR_TYPES =", detected)

    require(
        result.returncode == 0 and not detected,
        f"{label}_SQL_FAILED",
    )

    if explain:
        require("Physical Plan" in output, f"{label}_PLAN_MISSING")
        print(f"{label}_PLAN = PASSED")
        return

    job_ids = list(dict.fromkeys(
        re.findall(r"\b[0-9a-f]{32}\b", output)
    ))
    require(job_ids, f"{label}_JOB_ID_MISSING")

    job_id = job_ids[-1]

    for _ in range(60):
        with urlopen(
            f"http://127.0.0.1:8081/jobs/{job_id}",
            timeout=10,
        ) as response:
            state = json.load(response).get("state")

        if state == "FINISHED":
            print(f"{label}_JOB = FINISHED")
            return

        require(
            state not in {"FAILED", "CANCELED"},
            f"{label}_JOB_{state}",
        )
        time.sleep(2)

    raise RuntimeError(f"{label}_JOB_TIMEOUT")


def fixtures():
    """Constrói dados sintéticos sem identificadores pessoais."""
    base = {
        "source": "bluesky_jetstream_v2",
        "source_seq": 123,
        "collection": "app.bsky.feed.post",
        "operation": "create",
        "event_time": "2026-10-18T10:00:00.123456Z",
        "witnessed_at": "2026-10-18T10:00:01.123456Z",
    }

    def changed(**fields):
        return {**base, **fields}

    cases = [
        ("VALID", changed()),
        (
            "VALID",
            changed(
                event_time="2026-10-18T10:00:00Z",
                witnessed_at="2026-10-18T10:00:01Z",
            ),
        ),
        ("INVALID_JSON", "{invalid-json"),
        ("UNSUPPORTED_SOURCE", changed(source="other")),
        (
            "INVALID_COLLECTION",
            changed(collection="other.collection"),
        ),
        ("INVALID_OPERATION", changed(operation="unknown")),
        ("INVALID_SEQUENCE", changed(source_seq=-1)),
        (
            "INVALID_SEQUENCE",
            changed(source_seq="123"),
        ),
        (
            "INVALID_SEQUENCE",
            changed(source_seq=123.5),
        ),
        (
            "INVALID_SEQUENCE",
            changed(source_seq=True),
        ),
        (
            "INVALID_EVENT_TIME",
            changed(event_time="invalid"),
        ),
        (
            "INVALID_WITNESSED_AT",
            changed(witnessed_at="invalid"),
        ),
    ]

    bronze = read_rows(ROOT / "bronze/events_v1")
    require(bronze, "BRONZE_EMPTY")

    # Utilizamos somente um timestamp de transporte existente
    # no arquivo sintético. Seu valor não é exibido no terminal.
    kafka_timestamp = bronze[0]["kafka_timestamp"]

    FIXTURE_SOURCE.mkdir(parents=True, exist_ok=False)

    expected = {}
    rows = []

    for index, (reason, payload) in enumerate(cases):
        offset = 1000 + index

        expected[(0, offset)] = reason

        rows.append({
            "payload": (
                payload if isinstance(payload, str)
                else json.dumps(payload, separators=(",", ":"))
            ),
            "kafka_partition": 0,
            "kafka_offset": offset,
            "kafka_timestamp": kafka_timestamp,
        })

    destination = FIXTURE_SOURCE / "part-00000"

    with destination.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")

    print("CONTROLLED_CASES =", len(cases))
    return rows, expected


def verify_fixtures(source, expected):
    valid = read_rows(FIXTURE_VALID)
    rejected = read_rows(FIXTURE_QUARANTINE)

    reconcile(source, valid, rejected, "CONTRACT")

    observed = {}
    for row in valid:
        observed[position(row)] = "VALID"

    for row in rejected:
        observed[position(row)] = row["rejection_reason"]

    require(observed == expected, "CONTRACT_CLASSIFICATION_MISMATCH")

    print("CONTRACT_TEST = PASSED")


def verify_historical():
    bronze = read_rows(ROOT / "bronze/events_v1")
    valid = read_rows(HISTORICAL_VALID)
    rejected = read_rows(HISTORICAL_QUARANTINE)

    reconcile(bronze, valid, rejected, "HISTORICAL")

    reasons = Counter(
        row["rejection_reason"] for row in rejected
    )

    require(len(bronze) == 143, "BRONZE_COUNT_CHANGED")
    require(len(valid) == 102, "HISTORICAL_VALID_COUNT_CHANGED")
    require(len(rejected) == 41, "HISTORICAL_QUARANTINE_CHANGED")
    require(
        reasons == {"UNSUPPORTED_SOURCE": 41},
        "HISTORICAL_REJECTION_REASONS_CHANGED",
    )

    print("HISTORICAL_TEST = PASSED")


def main():
    print("=== SILVER REUSABLE VALIDATION ===", flush=True)
    print("RUN_ID =", RUN_ID, flush=True)

    require(not RUN_ROOT.exists(), "RUN_DIRECTORY_ALREADY_EXISTS")

    template = SQL_PATH.read_text(encoding="utf-8")

    require(
        "source_seq_json" in template,
        "NUMERIC_TYPE_VALIDATION_MISSING",
    )

    source, expected = fixtures()

    fixture_sql = adapt_sql(
        template,
        f"/silver_validation/{RUN_ID}/fixtures",
        f"/silver_validation/{RUN_ID}/fixture_valid",
        f"/silver_validation/{RUN_ID}/fixture_quarantine",
    )

    submit(fixture_sql, "FIXTURE_EXPLAIN", explain=True)
    submit(fixture_sql, "FIXTURE_EXECUTION")
    verify_fixtures(source, expected)

    historical_sql = adapt_sql(
        template,
        "/bronze/events_v1",
        f"/silver_validation/{RUN_ID}/historical_valid",
        f"/silver_validation/{RUN_ID}/historical_quarantine",
    )

    submit(historical_sql, "HISTORICAL_EXPLAIN", explain=True)
    submit(historical_sql, "HISTORICAL_EXECUTION")
    verify_historical()

    print("=== RESULT ===")
    print("SILVER_VALIDATION = PASSED")
    print("SQL_SOURCE_MODIFIED_BY_VALIDATOR = False")
    print("PREVIOUS_OUTPUTS_PRESERVED = True")


if __name__ == "__main__":
    main()
