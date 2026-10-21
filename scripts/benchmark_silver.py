"""Benchmark isolado Bronze Paimon -> Silver Paimon, 10 mil eventos.

Reutiliza a Bronze validada. Não publica eventos, não exclui
dados e não altera os SQLs de produção.
"""

import json
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
BRONZE_RUN = "6b9d03c76ba9"
BRONZE_JOURNAL = (
    ROOT / "runtime/benchmarks/bronze" / BRONZE_RUN / "status.json"
)
RUN_ID = uuid.uuid4().hex[:12]
DIRECTORY = ROOT / "runtime/benchmarks/silver" / RUN_ID
JOURNAL = DIRECTORY / "status.json"

SILVER_URI = (
    f"file:///opt/flink/runtime/benchmarks/silver/{RUN_ID}/warehouse"
)

EXPECTED_JOB_NAME = (
    "insert-into_silver_platform.social.silver_valid_sink,"
    "silver_platform.social.silver_quarantine_sink"
)

CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml", "exec", "-T",
    "flink-jobmanager", "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]

record = {
    "run_id": RUN_ID,
    "bronze_run": BRONZE_RUN,
    "silver_uri": SILVER_URI,
    "stage": "initialized",
}


def api(path):
    with urlopen("http://127.0.0.1:8081" + path, timeout=10) as response:
        return json.load(response)


def persist(stage):
    record["stage"] = stage
    temp = JOURNAL.with_suffix(".tmp")
    if temp.exists():
        raise RuntimeError("PENDING_JOURNAL_UPDATE")
    temp.write_text(json.dumps(record, indent=2), encoding="utf-8")
    temp.replace(JOURNAL)
    print("STAGE =", stage, flush=True)


def sql_client(sql, label):
    result = subprocess.run(
        CLIENT, cwd=ROOT, input=sql,
        text=True, capture_output=True, timeout=180,
    )
    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        diagnostics = [
            line[:250] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]
        print(label + "_ERROR_LINES =", diagnostics[-5:])
        raise RuntimeError(label + "_SQL_FAILED")

    return output


def replace_once(sql, old, new):
    if sql.count(old) != 1:
        raise RuntimeError("UNEXPECTED_SQL_TEMPLATE_STRUCTURE")
    return sql.replace(old, new, 1)


def read_counts(bronze_uri):
    # Consulta batch independente do job streaming.
    # Duas linhas são esperadas: VALID e QUARANTINE.
    query = f"""
SET 'parallelism.default' = '4';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG silver_audit WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{SILVER_URI}'
);

SELECT
 'VALID' AS layer,
 COUNT(*) AS total,
 COUNT(DISTINCT CONCAT(
    CAST(kafka_partition AS STRING), ':',
    CAST(kafka_offset AS STRING)
 )) AS unique_positions,
 COALESCE(SUM(CASE
    WHEN kafka_partition IS NULL OR kafka_offset IS NULL
    THEN 1 ELSE 0 END), 0) AS invalid_positions
FROM silver_audit.social.silver_valid_sink

UNION ALL

SELECT
 'QUARANTINE' AS layer,
 COUNT(*) AS total,
 COUNT(DISTINCT CONCAT(
    CAST(kafka_partition AS STRING), ':',
    CAST(kafka_offset AS STRING)
 )) AS unique_positions,
 COALESCE(SUM(CASE
    WHEN kafka_partition IS NULL OR kafka_offset IS NULL
    THEN 1 ELSE 0 END), 0) AS invalid_positions
FROM silver_audit.social.silver_quarantine_sink;
"""

    output = sql_client(query, "AUDIT")
    output = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", output)

    matches = re.findall(
        r"\|\s*(VALID|QUARANTINE)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*\|",
        output,
    )

    if len(matches) != 2:
        raise RuntimeError("UNEXPECTED_SILVER_AUDIT_FORMAT")

    counts = {}
    for layer, total, unique, invalid in matches:
        if layer in counts:
            raise RuntimeError("DUPLICATED_AUDIT_LAYER")
        counts[layer] = tuple(
            int(value.replace(",", ""))
            for value in (total, unique, invalid)
        )

    return counts


def stop_with_savepoint(job_id):
    persist("stop_requested")

    command = [
        "docker", "compose", "-p", "financial-streaming-platform",
        "-f", "compose.flink.yaml", "exec", "-T",
        "flink-jobmanager", "/opt/flink/bin/flink",
        "stop", "--savepointPath",
        "file:///opt/flink/runtime/savepoints", job_id,
    ]

    result = subprocess.run(
        command, cwd=ROOT, capture_output=True,
        text=True, timeout=180,
    )

    if result.returncode:
        raise RuntimeError("SAVEPOINT_STOP_FAILED")

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    match = re.search(
        r"file:/{1,3}opt/flink/runtime/savepoints/"
        r"(savepoint-[A-Za-z0-9-]+)",
        output,
    )

    if not match:
        raise RuntimeError("SAVEPOINT_NOT_IDENTIFIED")

    name = match.group(1)
    metadata = ROOT / "runtime/flink/savepoints" / name / "_metadata"

    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise RuntimeError("SAVEPOINT_METADATA_INVALID")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = api(f"/jobs/{job_id}").get("state")
        if status == "FINISHED":
            record["savepoint_name"] = name
            persist("stopped_with_savepoint")
            print("SAVEPOINT_NAME =", name)
            return
        if status in {"FAILED", "CANCELED"}:
            raise RuntimeError("UNEXPECTED_FINAL_JOB_STATE")
        time.sleep(2)

    raise RuntimeError("JOB_STOP_NOT_CONFIRMED")


def main():
    print("=== SILVER BENCHMARK 10K ===", flush=True)

    bronze = json.loads(
        BRONZE_JOURNAL.read_text(encoding="utf-8")
    )

    if (
        bronze.get("run_id") != BRONZE_RUN
        or bronze.get("stage") != "stopped_with_savepoint"
        or bronze.get("bronze_reconciliation") != "PASSED"
        or bronze.get("verified_rows") != 10000
        or bronze.get("verified_unique_positions") != 10000
        or bronze.get("verified_partitions") != 4
    ):
        raise RuntimeError("BRONZE_NOT_VALIDATED")

    bronze_uri = bronze["warehouse"]

    expected_bronze_uri = (
        "file:///opt/flink/runtime/benchmarks/bronze/"
        f"{BRONZE_RUN}/warehouse"
    )
    if bronze_uri != expected_bronze_uri:
        raise RuntimeError("BRONZE_WAREHOUSE_MISMATCH")

    overview = api("/overview")
    if overview.get("slots-available") != 8:
        raise RuntimeError("EIGHT_FREE_SLOTS_REQUIRED")

    before_jobs = api("/jobs/overview").get("jobs", [])
    active_states = {
        "CREATED", "INITIALIZING", "RUNNING",
        "RESTARTING", "RECONCILING", "FAILING",
        "CANCELLING", "SUSPENDED",
    }
    if any(job.get("state") in active_states for job in before_jobs):
        raise RuntimeError("ACTIVE_FLINK_JOBS_PRESENT")

    previous_ids = {job["jid"] for job in before_jobs}

    sql = (
        ROOT / "sql/08_paimon_bronze_to_silver.sql"
    ).read_text(encoding="utf-8")

    for old, new in (
        (
            "file:///opt/flink/runtime/paimon/platform_bronze_v1",
            bronze_uri,
        ),
        (
            "file:///opt/flink/runtime/paimon/platform_silver_v1",
            SILVER_URI,
        ),
        (
            "SET 'parallelism.default' = '1'",
            "SET 'parallelism.default' = '4'",
        ),
    ):
        sql = replace_once(sql, old, new)

    DIRECTORY.mkdir(parents=True, exist_ok=False)
    (DIRECTORY / "submitted_silver.sql").write_text(
        sql, encoding="utf-8"
    )

    record["bronze_uri"] = bronze_uri
    record["expected_rows"] = 10000
    persist("initialized")

    print("RUN_ID =", RUN_ID)
    print("BRONZE_RUN =", BRONZE_RUN)
    print("EXPECTED_VALID = 10000")
    print("EXPECTED_QUARANTINE = 0")
    print("FLINK_PARALLELISM = 4")

    persist("submission_started")
    start = time.monotonic()
    sql_client(sql, "SUBMISSION")

    job_id = None
    deadline = time.monotonic() + 90

    while time.monotonic() < deadline:
        candidates = [
            job for job in api("/jobs/overview").get("jobs", [])
            if job["jid"] not in previous_ids
        ]
        if len(candidates) > 1:
            raise RuntimeError("MULTIPLE_NEW_JOBS")
        if len(candidates) == 1:
            if candidates[0].get("name") != EXPECTED_JOB_NAME:
                raise RuntimeError("UNEXPECTED_JOB_NAME")
            job_id = candidates[0]["jid"]
            break
        time.sleep(2)

    if job_id is None:
        raise RuntimeError("JOB_NOT_IDENTIFIED")

    record["job_id"] = job_id
    persist("running")
    print("JOB_ID =", job_id)

    deadline = time.monotonic() + 240
    last_counts = None

    while time.monotonic() < deadline:
        job = api(f"/jobs/{job_id}")
        if job.get("state") != "RUNNING":
            raise RuntimeError(
                "SILVER_JOB_STATE_" + str(job.get("state"))
            )

        checkpoints = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if checkpoints < 1:
            time.sleep(3)
            continue

        counts = read_counts(bronze_uri)
        last_counts = counts

        valid_total, valid_unique, valid_invalid = counts["VALID"]
        quarantine_total, quarantine_unique, quarantine_invalid = (
            counts["QUARANTINE"]
        )

        print(
            "VALID =", valid_total,
            "QUARANTINE =", quarantine_total,
            "CHECKPOINTS =", checkpoints,
            flush=True,
        )

        if (
            valid_total > 10000
            or quarantine_total > 0
            or valid_unique > 10000
        ):
            raise RuntimeError("SILVER_UNEXPECTED_COUNTS")

        if (
            valid_total == 10000
            and valid_unique == 10000
            and valid_invalid == 0
            and quarantine_total == 0
            and quarantine_unique == 0
            and quarantine_invalid == 0
        ):
            break

        time.sleep(5)
    else:
        raise RuntimeError(
            "SILVER_RECONCILIATION_TIMEOUT_" + str(last_counts)
        )

    record["verified_valid_rows"] = 10000
    record["verified_quarantine_rows"] = 0
    record["reconciliation"] = "PASSED"
    record["seconds_to_reconciliation"] = round(
        time.monotonic() - start, 2
    )
    persist("reconciled")

    print("SILVER_RECONCILIATION = PASSED")
    print("SECONDS_TO_RECONCILIATION =", record["seconds_to_reconciliation"])

    stop_with_savepoint(job_id)
    persist("passed")

    print("FINAL_JOB_STATE = FINISHED")
    print("SILVER_BENCHMARK = PASSED")
    print("SILVER_VALID_ROWS = 10000")
    print("QUARANTINE_ROWS = 0")
    print("BRONZE_ROWS = 10000")
    print("DUPLICATE_KAFKA_POSITIONS = 0")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("SILVER_BENCHMARK = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:200])
        print("RUN_ID =", RUN_ID)
        print("DO_NOT_RERUN_WITHOUT_INSPECTING_JOURNAL = True")
        raise SystemExit(1)
