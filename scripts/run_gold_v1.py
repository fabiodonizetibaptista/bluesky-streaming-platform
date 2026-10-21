"""Executa a primeira materialização Gold sobre a Silver 100K.

Não remove dados, não reinicia containers e não altera
o contrato da Silver. O SQL é mantido separadamente.
"""

import json
import subprocess
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
JOURNAL = (
    ROOT / "runtime/flink/paimon/live_runs"
    / RUN_ID / "status.json"
)
SQL = ROOT / "sql/09_paimon_silver_to_gold_activity_1min.sql"

def main():
    report = json.loads(JOURNAL.read_text(encoding="utf-8"))

    if (
        report.get("run_id") != RUN_ID
        or report.get("stage") != "passed"
        or report.get("reconciliation", {}).get("valid") != 100000
    ):
        raise SystemExit("SILVER_SOURCE_NOT_VALIDATED")

    expected = (
        "file:///opt/flink/runtime/paimon/"
        f"live_runs/{RUN_ID}/silver"
    )
    if report.get("silver_uri") != expected:
        raise SystemExit("SILVER_WAREHOUSE_MISMATCH")

    with urlopen(
        "http://127.0.0.1:8081/jobs/overview", timeout=10
    ) as response:
        jobs = json.load(response).get("jobs", [])

    active = {
        "CREATED", "INITIALIZING", "RUNNING",
        "RESTARTING", "RECONCILING",
        "FAILING", "CANCELLING", "SUSPENDED",
    }

    if any(job.get("state") in active for job in jobs):
        raise SystemExit("ACTIVE_JOBS_PRESENT")

    sql = SQL.read_text(encoding="utf-8")

    print("=== GOLD MATERIALIZATION V1 ===", flush=True)
    print("SOURCE_RUN =", RUN_ID, flush=True)
    print("SOURCE_VALID_ROWS = 100000", flush=True)
    print("GOLD_GRAIN = one_minute", flush=True)

    command = [
        "docker", "compose",
        "-p", "financial-streaming-platform",
        "-f", "compose.flink.yaml",
        "exec", "-T", "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh",
        "-f", "/dev/stdin",
    ]

    # O registro local evita repetir automaticamente uma
    # materialização cujo resultado ainda não foi auditado.
    marker = (
        ROOT / "runtime/flink/paimon/live_runs"
        / RUN_ID / "gold_v1_submission_started"
    )

    if marker.exists():
        raise SystemExit(
            "GOLD_ALREADY_SUBMITTED_INSPECT_BEFORE_RETRY"
        )

    marker.touch(exist_ok=False)

    result = subprocess.run(
        command,
        cwd=ROOT,
        input=sql,
        capture_output=True,
        text=True,
        timeout=600,
    )

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        lines = [
            line[:220] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]

        print("SQL_ERROR_LINES =", lines[-5:])
        print("GOLD_MATERIALIZATION = INCOMPLETE")
        print("DO_NOT_RERUN = True")
        raise SystemExit(1)

    print("GOLD_SQL_EXECUTION = COMPLETED")
    print("NEXT_STEP = RECONCILE_GOLD_AGAINST_SILVER")

if __name__ == "__main__":
    main()
