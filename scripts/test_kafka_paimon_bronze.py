"""Teste real e isolado: coletor -> Kafka -> Flink -> Paimon Bronze.

Não usa o tópico de produção nem o checkpoint do coletor.
Não remove warehouses, tópicos, volumes ou savepoints.
"""

import json
import re
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient, NewTopic

from bluesky_streaming.checkpoint import CheckpointStore
from bluesky_streaming.collector import process_event
from bluesky_streaming.kafka_publisher import KafkaPublisher


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "sql/07_kafka_to_paimon_bronze.sql"
RUNTIME = ROOT / "runtime/flink/paimon"
ACTIVE_MARKER = RUNTIME / "bronze_integration_active.json"

FLINK_CLIENT = [
    "docker", "compose",
    "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]

ACTIVE_STATES = {
    "CREATED", "INITIALIZING", "RUNNING",
    "RESTARTING", "RECONCILING",
    "FAILING", "CANCELLING", "SUSPENDED",
}

EXPECTED_JOB = "insert-into_bronze_platform.social.bronze_events"


def flink_api(path):
    with urlopen(
        "http://127.0.0.1:8081" + path, timeout=10
    ) as response:
        return json.load(response)


def write_marker(record):
    """Grava o estado antes de operações não idempotentes."""
    ACTIVE_MARKER.parent.mkdir(parents=True, exist_ok=True)
    temporary = ACTIVE_MARKER.with_suffix(".json.tmp")
    if temporary.exists():
        raise RuntimeError("PENDING_MARKER_UPDATE")

    temporary.write_text(
        json.dumps(record, indent=2), encoding="utf-8"
    )
    temporary.replace(ACTIVE_MARKER)


def main():
    print("=== PREFLIGHT ===", flush=True)

    if not TEMPLATE.is_file():
        raise RuntimeError("BRONZE_SQL_TEMPLATE_MISSING")

    if ACTIVE_MARKER.exists():
        raise RuntimeError(
            "PREVIOUS_TEST_PRESENT_INSPECT_BEFORE_NEW_RUN"
        )

    jobs_before = flink_api("/jobs/overview").get("jobs", [])

    if any(j.get("state") in ACTIVE_STATES for j in jobs_before):
        raise RuntimeError("ACTIVE_FLINK_JOBS_PRESENT")

    overview = flink_api("/overview")

    if overview.get("slots-available") != 2:
        raise RuntimeError("TWO_FREE_SLOTS_REQUIRED")

    run_id = uuid.uuid4().hex[:12]
    topic = f"bluesky.bronze.test.{run_id}"

    warehouse = (
        f"file:///opt/flink/runtime/paimon/"
        f"bronze_integration/{run_id}"
    )

    warehouse_local = (
        RUNTIME / "bronze_integration" / run_id
    )

    if warehouse_local.exists():
        raise RuntimeError("WAREHOUSE_COLLISION")

    sql = TEMPLATE.read_text(encoding="utf-8")

    replacements = (
        (
            "'topic' = 'bluesky.posts.raw'",
            f"'topic' = '{topic}'",
        ),
        (
            "'warehouse' = "
            "'file:///opt/flink/runtime/paimon/platform_bronze_v1'",
            f"'warehouse' = '{warehouse}'",
        ),
    )

    for original, replacement in replacements:
        if sql.count(original) != 1:
            raise RuntimeError(
                "UNEXPECTED_SQL_TEMPLATE_CONTRACT"
            )
        sql = sql.replace(original, replacement, 1)

    record = {
        "run_id": run_id,
        "topic": topic,
        "warehouse": warehouse,
        "stage": "initialized",
    }
    write_marker(record)

    print("RUN_ID =", run_id)
    print("ISOLATED_TOPIC =", topic)
    print("ISOLATED_WAREHOUSE =", warehouse_local)

    print("=== CREATE TEST TOPIC ===", flush=True)

    admin = AdminClient({
        "bootstrap.servers": "127.0.0.1:9092",
    })

    futures = admin.create_topics([
        NewTopic(topic, num_partitions=1, replication_factor=1)
    ])
    futures[topic].result(timeout=20)

    record["stage"] = "topic_created"
    write_marker(record)

    print("TEST_TOPIC_CREATED = True")

    print("=== PUBLISH USING REAL COLLECTOR ===", flush=True)

    frame = {
        "$type": "message",
        "payload": {
            "$type": "network.bsky.jetstream.subscribeEvents#commit",
            "seq": 100001,
            "collection": "app.bsky.feed.post",
            "operation": "create",
            "time": "2026-10-20T20:00:00Z",
            "witnessedAt": "2026-10-20T20:00:01Z",
        },
    }

    with tempfile.TemporaryDirectory(
        prefix="bronze-integration-"
    ) as directory:
        checkpoint = CheckpointStore(
            Path(directory) / "cursor.json"
        )
        publisher = KafkaPublisher(topic=topic)

        process_event(frame, publisher.publish, checkpoint)

        if checkpoint.load() != 100001:
            raise RuntimeError("COLLECTOR_CHECKPOINT_MISMATCH")

    record["stage"] = "event_published"
    write_marker(record)

    print("REAL_KAFKA_PUBLICATION = PASSED")
    print("TEMPORARY_COLLECTOR_CHECKPOINT = PASSED")

    print("=== SUBMIT VERSIONED BRONZE SQL ===", flush=True)

    record["stage"] = "submission_started"
    write_marker(record)

    try:
        result = subprocess.run(
            FLINK_CLIENT,
            input=sql,
            text=True,
            capture_output=True,
            cwd=ROOT,
            timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            "SUBMISSION_TIMEOUT_DO_NOT_RERUN"
        )

    output = (result.stdout or "") + "\n" + (
        result.stderr or ""
    )

    errors = [
        marker
        for marker in (
            "[ERROR]",
            "ValidationException",
            "ClassNotFoundException",
            "SqlParserException",
        )
        if marker.lower() in output.lower()
    ]

    print("SQL_EXIT_CODE =", result.returncode)
    print("SQL_ERROR_MARKERS =", errors)

    if result.returncode != 0 or errors:
        print("SQL_DIAGNOSTIC =", output[-1200:])
        raise RuntimeError(
            "SQL_SUBMISSION_FAILED_INSPECT_BEFORE_RETRY"
        )

    print("=== IDENTIFY STREAMING JOB ===", flush=True)

    known_ids = {j["jid"] for j in jobs_before}
    deadline = time.monotonic() + 90

    while time.monotonic() < deadline:
        jobs = flink_api("/jobs/overview").get("jobs", [])
        matches = [
            j for j in jobs
            if j.get("jid") not in known_ids
            and j.get("name") == EXPECTED_JOB
        ]

        if len(matches) > 1:
            raise RuntimeError("AMBIGUOUS_STREAMING_JOBS")

        if len(matches) == 1:
            job_id = matches[0]["jid"]
            break

        time.sleep(2)
    else:
        raise RuntimeError(
            "STREAMING_JOB_NOT_FOUND_DO_NOT_RERUN"
        )

    record["job_id"] = job_id
    record["stage"] = "job_identified"
    write_marker(record)

    print("JOB_ID =", job_id)

    print("=== WAIT FOR STREAMING CHECKPOINT ===", flush=True)

    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        job = flink_api(f"/jobs/{job_id}")
        state = job.get("state")

        if state in {"FAILED", "FINISHED", "CANCELED"}:
            raise RuntimeError(
                f"UNEXPECTED_STREAMING_JOB_STATE_{state}"
            )

        checkpoints = flink_api(
            f"/jobs/{job_id}/checkpoints"
        )
        completed = checkpoints.get(
            "counts", {}
        ).get("completed", 0)

        if state == "RUNNING" and completed >= 1:
            record["stage"] = "checkpoint_validated"
            write_marker(record)

            print("JOB_STATE = RUNNING")
            print("COMPLETED_CHECKPOINTS =", completed)
            print("BRONZE_INTEGRATION_READY_FOR_READ = True")
            print("JOB_LEFT_RUNNING = True")
            print("DO_NOT_RERUN_TEST_SCRIPT = True")
            return

        time.sleep(3)

    raise RuntimeError("CHECKPOINT_TIMEOUT_INSPECT_JOB")


if __name__ == "__main__":
    main()
