"""Teste ao vivo: Bluesky -> Kafka -> Paimon Bronze -> Silver.

Usa o coletor original, com tópico e cursor isolados.
Não altera os SQLs oficiais e preserva todos os dados.
"""

import asyncio
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient, NewTopic


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = uuid.uuid4().hex[:12]
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
JOURNAL = RUN_DIR / "status.json"

TOPIC = f"bluesky.live.{RUN_ID}"
BRONZE = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/bronze"
SILVER = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/silver"

TARGET = 200

SQL_CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]

STOP_COMMAND = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/flink", "stop",
    "--savepointPath", "file:///opt/flink/runtime/savepoints",
]

ACTIVE = {
    "CREATED", "INITIALIZING", "RUNNING", "RESTARTING",
    "RECONCILING", "FAILING", "CANCELLING", "SUSPENDED",
}

state = {
    "run_id": RUN_ID,
    "topic": TOPIC,
    "bronze_uri": BRONZE,
    "silver_uri": SILVER,
    "target": TARGET,
    "jobs": {},
    "savepoints": {},
    "stage": "initialized",
}


def api(endpoint):
    with urlopen(
        "http://127.0.0.1:8081" + endpoint, timeout=10
    ) as response:
        return json.load(response)


def persist(stage):
    state["stage"] = stage
    temp = JOURNAL.with_suffix(".tmp")
    if temp.exists():
        raise RuntimeError("JOURNAL_UPDATE_PENDING")
    temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp.replace(JOURNAL)
    print("STAGE =", stage, flush=True)


def sql_client(sql, purpose):
    try:
        result = subprocess.run(
            SQL_CLIENT, cwd=ROOT, input=sql,
            text=True, capture_output=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(purpose + "_TIMEOUT")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        # Não exibir payloads ou SQL contendo eventos.
        print(purpose + "_EXIT_CODE =", result.returncode)
        print(purpose + "_SQL_ERROR =", "[ERROR]" in output)
        raise RuntimeError(purpose + "_FAILED")

    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", output)


def replace_once(sql, original, replacement):
    if sql.count(original) != 1:
        raise RuntimeError("SQL_TEMPLATE_CONTRACT_CHANGED")
    return sql.replace(original, replacement, 1)


def build_sql(filename, substitutions):
    sql = (ROOT / "sql" / filename).read_text(encoding="utf-8")
    for original, replacement in substitutions:
        sql = replace_once(sql, original, replacement)
    return sql


def submit_job(label, sql, expected_name):
    old_ids = {
        item["jid"] for item in api("/jobs/overview").get("jobs", [])
    }

    persist(label + "_submission_started")
    sql_client(sql, label + "_SUBMISSION")

    deadline = time.monotonic() + 90

    while time.monotonic() < deadline:
        new = [
            item for item in api("/jobs/overview").get("jobs", [])
            if item["jid"] not in old_ids
        ]
        if len(new) > 1:
            raise RuntimeError(label + "_AMBIGUOUS_JOBS")
        if len(new) == 1:
            if new[0].get("name") != expected_name:
                raise RuntimeError(label + "_UNEXPECTED_JOB_NAME")
            job_id = new[0]["jid"]
            state["jobs"][label] = job_id
            persist(label + "_submitted")
            print(label + "_JOB_ID =", job_id)
            return job_id

        time.sleep(2)

    raise RuntimeError(label + "_JOB_NOT_IDENTIFIED")


def wait_checkpoint(job_id, label):
    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        job = api(f"/jobs/{job_id}")
        status = job.get("state")

        if status in {"FAILED", "CANCELED", "FINISHED"}:
            raise RuntimeError(label + "_JOB_" + status)

        checkpoints = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if status == "RUNNING" and checkpoints >= 1:
            print(label + "_CHECKPOINTS =", checkpoints)
            return

        time.sleep(3)

    raise RuntimeError(label + "_CHECKPOINT_TIMEOUT")


def audit():
    query = f"""
SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG bronze_audit WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{BRONZE}'
);

CREATE CATALOG silver_audit WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{SILVER}'
);

SELECT
 'BRONZE' AS layer,
 COUNT(*) AS total,
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 )) AS unique_positions
FROM bronze_audit.social.bronze_events

UNION ALL

SELECT
 'VALID',
 COUNT(*),
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 ))
FROM silver_audit.social.silver_valid_sink

UNION ALL

SELECT
 'QUARANTINE',
 COUNT(*),
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 ))
FROM silver_audit.social.silver_quarantine_sink;
"""

    output = sql_client(query, "LIVE_AUDIT")

    matches = re.findall(
        r"\|\s*(BRONZE|VALID|QUARANTINE)\s*"
        r"\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|",
        output,
    )

    if len(matches) != 3:
        raise RuntimeError("AUDIT_RESULT_FORMAT_UNEXPECTED")

    counts = {}
    for layer, total, unique in matches:
        if layer in counts:
            raise RuntimeError("AUDIT_DUPLICATED_LAYER")
        counts[layer] = (
            int(total.replace(",", "")),
            int(unique.replace(",", "")),
        )

    return counts


def reconcile(bronze_job, silver_job):
    deadline = time.monotonic() + 240

    while time.monotonic() < deadline:
        for label, job_id in (
            ("BRONZE", bronze_job),
            ("SILVER", silver_job),
        ):
            if api(f"/jobs/{job_id}").get("state") != "RUNNING":
                raise RuntimeError(label + "_NOT_RUNNING")

        counts = audit()
        bronze_total, bronze_unique = counts["BRONZE"]
        valid_total, valid_unique = counts["VALID"]
        quarantine_total, quarantine_unique = counts["QUARANTINE"]

        print(
            "BRONZE =", bronze_total,
            "VALID =", valid_total,
            "QUARANTINE =", quarantine_total,
            flush=True,
        )

        if (
            bronze_total > TARGET
            or valid_total + quarantine_total > TARGET
            or bronze_unique != bronze_total
            or valid_unique != valid_total
            or quarantine_unique != quarantine_total
        ):
            raise RuntimeError("LIVE_RECONCILIATION_INVALID")

        # A soma dos destinos precisa corresponder à Bronze,
        # mas só aprovamos quando todos os eventos chegarem.
        if (
            bronze_total == TARGET
            and valid_total + quarantine_total == TARGET
        ):
            state["reconciliation"] = {
                "bronze": bronze_total,
                "valid": valid_total,
                "quarantine": quarantine_total,
            }
            persist("reconciled")
            print("LIVE_RECONCILIATION = PASSED")
            return

        time.sleep(8)

    raise RuntimeError("LIVE_RECONCILIATION_TIMEOUT")


def stop_job(label, job_id):
    if api(f"/jobs/{job_id}").get("state") != "RUNNING":
        raise RuntimeError(label + "_NOT_RUNNING_BEFORE_STOP")

    persist(label + "_stop_requested")

    try:
        result = subprocess.run(
            STOP_COMMAND + [job_id],
            cwd=ROOT, text=True, capture_output=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(label + "_STOP_TIMEOUT")

    if result.returncode:
        raise RuntimeError(label + "_STOP_FAILED")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    match = re.search(
        r"file:/{1,3}opt/flink/runtime/savepoints/"
        r"(savepoint-[A-Za-z0-9-]+)",
        output,
    )

    if not match:
        raise RuntimeError(label + "_SAVEPOINT_NOT_IDENTIFIED")

    name = match.group(1)
    metadata = ROOT / "runtime/flink/savepoints" / name / "_metadata"

    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise RuntimeError(label + "_SAVEPOINT_METADATA_INVALID")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = api(f"/jobs/{job_id}").get("state")
        if status == "FINISHED":
            state["savepoints"][label] = name
            persist(label + "_stopped")
            print(label + "_STOP = PASSED")
            return
        if status in {"FAILED", "CANCELED"}:
            raise RuntimeError(label + "_UNEXPECTED_TERMINATION")
        time.sleep(2)

    raise RuntimeError(label + "_STOP_NOT_CONFIRMED")


def main():
    print("=== BLUESKY LIVE STREAMING ===", flush=True)

    if Path.cwd().resolve() != ROOT:
        raise RuntimeError("RUN_FROM_REPOSITORY_ROOT")

    overview = api("/overview")
    jobs = api("/jobs/overview").get("jobs", [])

    if overview.get("slots-available") != 8:
        raise RuntimeError("EIGHT_FREE_SLOTS_REQUIRED")

    if any(item.get("state") in ACTIVE for item in jobs):
        raise RuntimeError("OTHER_ACTIVE_JOBS_PRESENT")

    RUN_DIR.mkdir(parents=True, exist_ok=False)
    persist("initialized")

    print("RUN_ID =", RUN_ID)
    print("TOPIC =", TOPIC)
    print("TARGET_EVENTS =", TARGET)

    # Um tópico exclusivo com uma partição simplifica a
    # reconciliação entre ACKs e offsets Kafka.
    persist("topic_creation_started")
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    admin.create_topics([
        NewTopic(
            TOPIC, num_partitions=1, replication_factor=1,
            config={"retention.ms": "86400000"},
        )
    ])[TOPIC].result(timeout=30)
    persist("topic_created")

    bronze_sql = build_sql(
        "07_kafka_to_paimon_bronze.sql",
        [
            ("'topic' = 'bluesky.posts.raw'", f"'topic' = '{TOPIC}'"),
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE,
            ),
        ],
    )

    silver_sql = build_sql(
        "08_paimon_bronze_to_silver.sql",
        [
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE,
            ),
            (
                "file:///opt/flink/runtime/paimon/platform_silver_v1",
                SILVER,
            ),
        ],
    )

    (RUN_DIR / "bronze.sql").write_text(
        bronze_sql, encoding="utf-8"
    )
    (RUN_DIR / "silver.sql").write_text(
        silver_sql, encoding="utf-8"
    )

    bronze_job = submit_job(
        "BRONZE", bronze_sql,
        "insert-into_bronze_platform.social.bronze_events",
    )
    wait_checkpoint(bronze_job, "BRONZE")

    silver_job = submit_job(
        "SILVER", silver_sql,
        "insert-into_silver_platform.social.silver_valid_sink,"
        "silver_platform.social.silver_quarantine_sink",
    )
    wait_checkpoint(silver_job, "SILVER")

    # Confirma que ambos os jobs estão ativos antes de conectar
    # o coletor à fonte real de eventos.
    persist("both_jobs_streaming")
    print("BOTH_FLINK_JOBS_RUNNING = True")

    # Apenas esta execução recebe o tópico e cursor isolados.
    # Não alteramos os arquivos de código do coletor.
    os.environ["KAFKA_TOPIC"] = TOPIC

    from bluesky_streaming import ingest

    ingest.CHECKPOINT_PATH = str(RUN_DIR / "live_cursor.json")
    ingest.MAX_EVENTS = TARGET

    print("=== CONNECTING REAL JETSTREAM ===", flush=True)
    persist("collector_started")
    asyncio.run(ingest.ingest_events())

    state["collector_confirmed"] = TARGET
    persist("collector_completed")
    print("REAL_EVENTS_CONFIRMED =", TARGET)

    print("=== INDEPENDENT PAIMON RECONCILIATION ===", flush=True)
    reconcile(bronze_job, silver_job)

    # Encerra primeiro o consumidor downstream e depois a Bronze,
    # preservando savepoints para ambas as camadas.
    stop_job("SILVER", silver_job)
    stop_job("BRONZE", bronze_job)

    persist("passed")
    print("LIVE_PIPELINE_RESULT = PASSED")
    print("LIVE_RUN_ID =", RUN_ID)
    print("BRONZE_ROWS =", state["reconciliation"]["bronze"])
    print("SILVER_VALID_ROWS =", state["reconciliation"]["valid"])
    print("QUARANTINE_ROWS =", state["reconciliation"]["quarantine"])
    print("BOTH_JOBS_STOPPED_WITH_SAVEPOINT = True")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("LIVE_PIPELINE_RESULT = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:180])
        print("LIVE_RUN_ID =", RUN_ID)
        print("INSPECT_JOURNAL_BEFORE_ANY_RETRY = True")
        raise SystemExit(1)
