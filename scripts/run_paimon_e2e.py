"""Integração isolada: Kafka -> Paimon Bronze -> Silver/Quarentena.

Executar a partir da raiz do repositório:
    .venv/bin/python scripts/run_paimon_e2e.py

Cada execução cria tópico, warehouses e journal próprios.
Nenhum dado histórico ou volume é removido automaticamente.
"""

import json
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient, NewTopic

from bluesky_streaming.checkpoint import CheckpointStore
from bluesky_streaming.collector import process_event
from bluesky_streaming.kafka_publisher import KafkaPublisher


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = uuid.uuid4().hex[:12]
RUN_DIR = ROOT / "runtime/flink/paimon/e2e_runs" / RUN_ID
JOURNAL = RUN_DIR / "status.json"
TOPIC = f"bluesky.e2e.{RUN_ID}"
JOB_BRONZE = "insert-into_bronze_platform.social.bronze_events"
JOB_SILVER = (
    "insert-into_silver_platform.social.silver_valid_sink,"
    "silver_platform.social.silver_quarantine_sink"
)

BRONZE_URI = (
    f"file:///opt/flink/runtime/paimon/e2e_runs/{RUN_ID}/bronze"
)
SILVER_URI = (
    f"file:///opt/flink/runtime/paimon/e2e_runs/{RUN_ID}/silver"
)

CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml", "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]
ACTIVE = {
    "CREATED", "INITIALIZING", "RUNNING", "RESTARTING",
    "RECONCILING", "FAILING", "CANCELLING", "SUSPENDED",
}
state = {
    "run_id": RUN_ID,
    "topic": TOPIC,
    "bronze_uri": BRONZE_URI,
    "silver_uri": SILVER_URI,
    "stage": "initialized",
    "jobs": {},
    "savepoints": {},
}


def api(endpoint):
    with urlopen("http://127.0.0.1:8081" + endpoint, timeout=10) as response:
        return json.load(response)


def persist(stage):
    """Registrar transições antes e depois de operações de estado."""
    state["stage"] = stage
    temp = JOURNAL.with_suffix(".tmp")
    if temp.exists():
        raise RuntimeError("JOURNAL_UPDATE_ALREADY_PENDING")
    temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp.replace(JOURNAL)
    print("STAGE =", stage, flush=True)


def sql_client(sql, label):
    try:
        result = subprocess.run(
            CLIENT, input=sql, cwd=ROOT,
            capture_output=True, text=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{label}_TIMEOUT_STATE_UNKNOWN")

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    errors = [
        token for token in (
            "[ERROR]", "ValidationException",
            "ClassNotFoundException", "SqlParserException",
            "SQL execution error",
        )
        if token.lower() in output.lower()
    ]

    if result.returncode or errors:
        print(label + "_ERROR_MARKERS =", errors)
        # Diagnóstico do SQL Client sem imprimir payloads ou SQL.
        lines = [
            line[:250] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]
        print(label + "_ERROR_LINES =", lines[-5:])
        raise RuntimeError(f"{label}_FAILED")

    return output


def replace_exact(sql, old, new):
    if sql.count(old) != 1:
        raise RuntimeError("UNEXPECTED_SQL_TEMPLATE_CONTRACT")
    return sql.replace(old, new, 1)


def template_sql(filename, substitutions):
    sql = (ROOT / "sql" / filename).read_text(encoding="utf-8")
    for old, new in substitutions:
        sql = replace_exact(sql, old, new)
    return sql


def submit(sql, expected_name, label):
    before = {
        item["jid"] for item in api("/jobs/overview").get("jobs", [])
    }
    persist(label + "_submission_started")
    sql_client(sql, label + "_SUBMISSION")

    deadline = time.monotonic() + 90
    job_id = None

    while time.monotonic() < deadline:
        candidates = [
            item for item in api("/jobs/overview").get("jobs", [])
            if item["jid"] not in before
            and item.get("name") == expected_name
        ]
        if len(candidates) > 1:
            raise RuntimeError("AMBIGUOUS_NEW_JOBS")
        if len(candidates) == 1:
            job_id = candidates[0]["jid"]
            break
        time.sleep(2)

    if not job_id:
        raise RuntimeError("JOB_NOT_IDENTIFIED_DO_NOT_RESUBMIT")

    state["jobs"][label] = job_id
    persist(label + "_submitted")
    print(label + "_JOB_ID =", job_id)
    return job_id


def wait_checkpoint(job_id, label):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        status = api(f"/jobs/{job_id}").get("state")
        if status in {"FAILED", "FINISHED", "CANCELED"}:
            raise RuntimeError(f"{label}_UNEXPECTED_JOB_STATE_{status}")

        completed = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if status == "RUNNING" and completed >= 1:
            print(label + "_CHECKPOINTS =", completed)
            return
        time.sleep(3)

    raise RuntimeError(f"{label}_CHECKPOINT_TIMEOUT")


def query_rows(sql, label):
    output = sql_client(sql, label)
    parsed = []

    for line in output.splitlines():
        if not line.startswith("|"):
            continue
        columns = [part.strip() for part in line.strip().strip("|").split("|")]
        if columns and columns[0] in {
            "BRONZE", "VALID", "QUARANTINE"
        }:
            parsed.append(tuple(columns))
    return parsed


def wait_rows(sql, expected, label):
    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        rows = query_rows(sql, label)
        if sorted(rows) == sorted(expected):
            print(label + "_ROWS =", len(rows))
            print(label + "_RECONCILIATION = PASSED")
            return

        # Qualquer resultado inesperado que não seja apenas um
        # subconjunto do esperado interrompe o teste.
        if len(rows) > len(expected) or not set(rows).issubset(set(expected)):
            print(label + "_OBSERVED_COUNT =", len(rows))
            raise RuntimeError(label + "_UNEXPECTED_RESULTS")

        time.sleep(5)

    raise RuntimeError(label + "_RECONCILIATION_TIMEOUT")


def stop_with_savepoint(job_id, label):
    if api(f"/jobs/{job_id}").get("state") != "RUNNING":
        raise RuntimeError(f"{label}_NOT_RUNNING_BEFORE_STOP")

    persist(label + "_stop_requested")
    command = [
        "docker", "compose", "-p", "financial-streaming-platform",
        "-f", "compose.flink.yaml", "exec", "-T", "flink-jobmanager",
        "/opt/flink/bin/flink", "stop",
        "--savepointPath", "file:///opt/flink/runtime/savepoints",
        job_id,
    ]

    try:
        result = subprocess.run(
            command, cwd=ROOT,
            capture_output=True, text=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{label}_STOP_TIMEOUT_DO_NOT_REPEAT")

    if result.returncode:
        raise RuntimeError(f"{label}_STOP_FAILED_INSPECT_JOB")

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    match = re.search(
        r"file:/{1,3}opt/flink/runtime/savepoints/"
        r"(savepoint-[A-Za-z0-9-]+)",
        output,
    )
    if not match:
        raise RuntimeError(f"{label}_SAVEPOINT_PATH_NOT_IDENTIFIED")

    name = match.group(1)
    metadata = ROOT / "runtime/flink/savepoints" / name / "_metadata"

    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise RuntimeError(f"{label}_SAVEPOINT_METADATA_INVALID")

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        status = api(f"/jobs/{job_id}").get("state")
        if status == "FINISHED":
            break
        if status in {"CANCELED", "FAILED"}:
            raise RuntimeError(f"{label}_UNEXPECTED_FINAL_STATE")
        time.sleep(2)
    else:
        raise RuntimeError(f"{label}_STOP_NOT_CONFIRMED")

    state["savepoints"][label] = name
    persist(label + "_stopped")
    print(label + "_STOP = PASSED")


def catalog_prefix():
    return f"""
SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'table.local-time-zone' = 'UTC';

CREATE CATALOG bronze_platform WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{BRONZE_URI}'
);
CREATE CATALOG silver_platform WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{SILVER_URI}'
);
USE CATALOG bronze_platform;
USE social;
"""


def publish_fixture():
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    persist("topic_creation_started")
    futures = admin.create_topics([
        NewTopic(TOPIC, num_partitions=1, replication_factor=1)
    ])
    futures[TOPIC].result(timeout=20)
    persist("topic_created")

    publisher = KafkaPublisher(topic=TOPIC)
    checkpoint = CheckpointStore(RUN_DIR / "collector_cursor.json")

    frame = {
        "$type": "message",
        "payload": {
            "seq": 100001,
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "time": "2026-10-20T20:00:00Z",
            "witnessedAt": "2026-10-20T20:00:01Z",
        },
    }

    persist("fixture_publication_started")
    process_event(frame, publisher.publish, checkpoint)

    if checkpoint.load() != 100001:
        raise RuntimeError("COLLECTOR_CHECKPOINT_MISMATCH")

    common = {
        "source": "bluesky_jetstream_v2",
        "collection": "app.bsky.feed.post",
        "operation": "create",
        "event_time": "2026-10-20T20:00:00Z",
        "witnessed_at": "2026-10-20T20:00:01Z",
    }

    # Os quatro registros seguintes exercitam regras de quarentena.
    invalid_events = [
        b"{invalid-json",
        json.dumps({**common, "source": "unsupported", "source_seq": 100003}).encode(),
        json.dumps({**common, "operation": "unknown", "source_seq": 100004}).encode(),
        json.dumps({**common, "source_seq": "bad-sequence"}).encode(),
    ]

    for event in invalid_events:
        publisher.publish(event)

    persist("fixture_published")
    print("KAFKA_EVENTS_PUBLISHED = 5")


def main():
    print("=== PAIMON E2E ===", flush=True)
    if Path.cwd().resolve() != ROOT:
        raise RuntimeError("RUN_FROM_REPOSITORY_ROOT")

    for filename in (
        "sql/07_kafka_to_paimon_bronze.sql",
        "sql/08_paimon_bronze_to_silver.sql",
    ):
        if not (ROOT / filename).is_file():
            raise RuntimeError("SQL_TEMPLATE_MISSING")

    jobs = api("/jobs/overview").get("jobs", [])
    if any(job.get("state") in ACTIVE for job in jobs):
        raise RuntimeError("ACTIVE_FLINK_JOB_PRESENT")

    if api("/overview").get("slots-available") != 2:
        raise RuntimeError("TWO_FREE_SLOTS_REQUIRED")

    RUN_DIR.mkdir(parents=True, exist_ok=False)
    persist("initialized")
    print("RUN_ID =", RUN_ID)
    print("JOURNAL =", JOURNAL.relative_to(ROOT))

    publish_fixture()

    bronze_sql = template_sql(
        "07_kafka_to_paimon_bronze.sql",
        [
            ("'topic' = 'bluesky.posts.raw'", f"'topic' = '{TOPIC}'"),
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE_URI,
            ),
        ],
    )

    bronze_job = submit(bronze_sql, JOB_BRONZE, "BRONZE")
    wait_checkpoint(bronze_job, "BRONZE")

    prefix = catalog_prefix()

    bronze_read = prefix + """
SELECT 'BRONZE', CAST(kafka_partition AS STRING),
       CAST(kafka_offset AS STRING)
FROM bronze_platform.social.bronze_events
ORDER BY kafka_offset;
"""
    wait_rows(
        bronze_read,
        [("BRONZE", "0", str(i)) for i in range(5)],
        "BRONZE",
    )
    persist("bronze_data_validated")

    stop_with_savepoint(bronze_job, "BRONZE")

    silver_sql = template_sql(
        "08_paimon_bronze_to_silver.sql",
        [
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE_URI,
            ),
            (
                "file:///opt/flink/runtime/paimon/platform_silver_v1",
                SILVER_URI,
            ),
        ],
    )

    silver_job = submit(silver_sql, JOB_SILVER, "SILVER")
    wait_checkpoint(silver_job, "SILVER")

    reconciliation_sql = prefix + """
USE CATALOG silver_platform;
USE social;

SELECT 'VALID', CAST(kafka_offset AS STRING),
       'VALID', 'NONE'
FROM silver_valid_sink
UNION ALL
SELECT 'QUARANTINE', CAST(kafka_offset AS STRING),
       quality_status, rejection_reason
FROM silver_quarantine_sink
ORDER BY 2;
"""
    expected = [
        ("VALID", "0", "VALID", "NONE"),
        ("QUARANTINE", "1", "INVALID_JSON", "INVALID_JSON"),
        ("QUARANTINE", "2", "UNSUPPORTED_SOURCE", "UNSUPPORTED_SOURCE"),
        ("QUARANTINE", "3", "INVALID_CONTRACT", "INVALID_OPERATION"),
        ("QUARANTINE", "4", "INVALID_CONTRACT", "INVALID_SEQUENCE"),
    ]
    wait_rows(reconciliation_sql, expected, "SILVER")
    persist("silver_data_validated")

    stop_with_savepoint(silver_job, "SILVER")

    persist("passed")
    print("E2E_RESULT = PASSED")
    print("BRONZE_RECORDS = 5")
    print("SILVER_VALID_RECORDS = 1")
    print("QUARANTINE_RECORDS = 4")
    print("BOTH_JOBS_STOPPED_WITH_SAVEPOINT = True")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("E2E_RESULT = FAILED_OR_INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:200])
        print("RUN_ID =", RUN_ID)
        print("DO_NOT_DELETE_RUN_DATA = True")
        print("INSPECT_JOURNAL_AND_FLINK_JOBS_BEFORE_RETRY = True")
        raise SystemExit(1)
