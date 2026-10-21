"""Benchmark Kafka -> Paimon Bronze usando tópico sintético existente.

Não publica eventos, não altera o SQL de produção e não
remove dados. Preserva o job para inspeção e recuperação.
"""

import json
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
SOURCE_RUN = "ada7438ba564"
SOURCE_REPORT = ROOT / "runtime/benchmarks" / f"{SOURCE_RUN}.json"

CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml", "exec", "-T",
    "flink-jobmanager", "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]

def api(path):
    with urlopen("http://127.0.0.1:8081" + path, timeout=10) as response:
        return json.load(response)

def replace_once(sql, old, new):
    if sql.count(old) != 1:
        raise RuntimeError("UNEXPECTED_SQL_TEMPLATE")
    return sql.replace(old, new, 1)

def submit_sql(sql):
    result = subprocess.run(
        CLIENT, input=sql, text=True,
        capture_output=True, timeout=180,
    )
    output = (result.stdout or "") + "\n" + (result.stderr or "")
    if result.returncode or "[ERROR]" in output:
        lines = [
            line[:200] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]
        print("SQL_ERRORS =", lines[-5:])
        raise RuntimeError("SQL_SUBMISSION_FAILED")

print("=== BRONZE BENCHMARK PREFLIGHT ===", flush=True)

report = json.loads(SOURCE_REPORT.read_text(encoding="utf-8"))
if (
    report["status"] != "PASSED"
    or report["events"] != 10000
    or report["partitions"] != 4
):
    raise SystemExit("SOURCE_BENCHMARK_MISMATCH")

overview = api("/overview")
if overview.get("slots-available") != 8:
    raise SystemExit("EIGHT_FREE_SLOTS_REQUIRED")

jobs = api("/jobs/overview").get("jobs", [])
active = {
    "CREATED", "INITIALIZING", "RUNNING",
    "RESTARTING", "RECONCILING", "FAILING",
    "CANCELLING", "SUSPENDED",
}
if any(job.get("state") in active for job in jobs):
    raise SystemExit("OTHER_ACTIVE_JOBS_PRESENT")

run_id = uuid.uuid4().hex[:12]
directory = ROOT / "runtime/benchmarks/bronze" / run_id
directory.mkdir(parents=True, exist_ok=False)

warehouse = (
    f"file:///opt/flink/runtime/benchmarks/bronze/"
    f"{run_id}/warehouse"
)

journal_path = directory / "status.json"
journal = {
    "run_id": run_id,
    "source_run": SOURCE_RUN,
    "topic": report["topic"],
    "warehouse": warehouse,
    "stage": "initialized",
}

def save(stage):
    journal["stage"] = stage
    temporary = journal_path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(journal, indent=2), encoding="utf-8"
    )
    temporary.replace(journal_path)
    print("STAGE =", stage, flush=True)

sql = (ROOT / "sql/07_kafka_to_paimon_bronze.sql").read_text(
    encoding="utf-8"
)

sql = replace_once(
    sql,
    "'topic' = 'bluesky.posts.raw'",
    f"'topic' = '{report['topic']}'",
)
sql = replace_once(
    sql,
    "file:///opt/flink/runtime/paimon/platform_bronze_v1",
    warehouse,
)
sql = replace_once(
    sql,
    "SET 'parallelism.default' = '1'",
    "SET 'parallelism.default' = '4'",
)

(directory / "submitted_bronze.sql").write_text(
    sql, encoding="utf-8"
)

previous_ids = {job["jid"] for job in jobs}

print("RUN_ID =", run_id)
print("SOURCE_EVENTS =", report["events"])
print("KAFKA_PARTITIONS =", report["partitions"])
print("FLINK_PARALLELISM = 4")

save("submission_started")
started = time.monotonic()
submit_sql(sql)

deadline = time.monotonic() + 90
job_id = None

while time.monotonic() < deadline:
    candidates = [
        job for job in api("/jobs/overview").get("jobs", [])
        if job["jid"] not in previous_ids
    ]
    if len(candidates) > 1:
        raise SystemExit("MULTIPLE_NEW_JOBS_INSPECT_MANUALLY")
    if len(candidates) == 1:
        job_id = candidates[0]["jid"]
        break
    time.sleep(2)

if job_id is None:
    raise SystemExit("JOB_NOT_IDENTIFIED_DO_NOT_RESUBMIT")

journal["job_id"] = job_id
save("running")
print("JOB_ID =", job_id)

deadline = time.monotonic() + 180

while time.monotonic() < deadline:
    job = api(f"/jobs/{job_id}")
    state = job.get("state")

    if state in {"FAILED", "CANCELED", "FINISHED"}:
        raise SystemExit("UNEXPECTED_JOB_STATE_" + state)

    checkpoints = api(
        f"/jobs/{job_id}/checkpoints"
    ).get("counts", {}).get("completed", 0)

    if checkpoints >= 2 and state == "RUNNING":
        break

    time.sleep(3)
else:
    raise SystemExit("CHECKPOINT_TIMEOUT")

elapsed = time.monotonic() - started

print("JOB_STATE = RUNNING")
print("COMPLETED_CHECKPOINTS =", checkpoints)
print("SECONDS_TO_TWO_CHECKPOINTS =", round(elapsed, 2))
print("BRONZE_JOB_SUBMITTED = PASSED")
print("NEXT_STEP = RECONCILE_10000_ROWS_AND_STOP_WITH_SAVEPOINT")
print("DO_NOT_RERUN_SCRIPT = True")
