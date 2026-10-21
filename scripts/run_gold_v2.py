"""Materializa e audita a Gold V2 contra a Silver original.

Preserva V1 e V2; impede submissão automática repetida.
Não imprime dados individuais nem credenciais.
"""

import json
import re
import subprocess
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
SILVER = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/silver"
GOLD = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/gold_v2"
MARKER = RUN_DIR / "gold_v2_submission_started"
AUDIT_PATH = RUN_DIR / "gold_v2_audit.json"

CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml", "exec", "-T",
    "flink-jobmanager", "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]


def execute(sql, label, timeout=600):
    result = subprocess.run(
        CLIENT, cwd=ROOT, input=sql, text=True,
        capture_output=True, timeout=timeout,
    )
    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        print(label + "_EXIT_CODE =", result.returncode)
        print(label + "_SQL_ERROR =", "[ERROR]" in output)
        raise RuntimeError(label + "_FAILED")

    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", output)


def preflight():
    report = json.loads(
        (RUN_DIR / "status.json").read_text(encoding="utf-8")
    )

    if (
        report.get("stage") != "passed"
        or report.get("silver_uri") != SILVER
        or report.get("reconciliation", {}).get("valid") != 100000
    ):
        raise RuntimeError("SILVER_SOURCE_NOT_VALIDATED")

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
        raise RuntimeError("OTHER_ACTIVE_JOBS_PRESENT")


def audit():
    # Compara a quantidade em cada minuto. Não apenas os totais.
    # FULL OUTER JOIN detecta minutos ausentes e inesperados.
    sql = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '4';

CREATE CATALOG silver_audit WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{SILVER}'
);

CREATE CATALOG gold_audit WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{GOLD}'
);

CREATE TEMPORARY VIEW expected_minutes AS
SELECT
    CAST(
        CONCAT(
            DATE_FORMAT(event_time_utc, 'yyyy-MM-dd HH:mm'),
            ':00'
        ) AS TIMESTAMP(6)
    ) AS minute_start,
    COUNT(*) AS expected_count,
    SUM(CASE WHEN operation_name = 'create'
        THEN 1 ELSE 0 END) AS expected_creates,
    SUM(CASE WHEN operation_name = 'update'
        THEN 1 ELSE 0 END) AS expected_updates,
    SUM(CASE WHEN operation_name = 'delete'
        THEN 1 ELSE 0 END) AS expected_deletes
FROM silver_audit.social.silver_valid_sink
GROUP BY
    CAST(
        CONCAT(
            DATE_FORMAT(event_time_utc, 'yyyy-MM-dd HH:mm'),
            ':00'
        ) AS TIMESTAMP(6)
    );

SELECT
    COUNT(*) AS compared_minutes,
    COALESCE(SUM(g.total_events), 0) AS gold_events,
    COALESCE(SUM(e.expected_count), 0) AS silver_events,
    COALESCE(SUM(
        CASE
            WHEN e.minute_start IS NULL
              OR g.window_start IS NULL
              OR g.total_events <> e.expected_count
              OR g.create_count <> e.expected_creates
              OR g.update_count <> e.expected_updates
              OR g.delete_count <> e.expected_deletes
            THEN 1 ELSE 0
        END
    ), 0) AS mismatched_minutes,
    COALESCE(SUM(
        CASE
            WHEN g.window_start IS NOT NULL
             AND (g.window_end IS NULL
               OR TIMESTAMPDIFF(
                    SECOND, g.window_start, g.window_end
                  ) <> 60)
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_durations
FROM expected_minutes e
FULL OUTER JOIN gold_audit.social.gold_activity_1min g
    ON e.minute_start = g.window_start;
"""

    output = execute(sql, "GOLD_V2_AUDIT", timeout=300)

    matches = re.findall(
        r"\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*\|",
        output,
    )

    if len(matches) != 1:
        raise RuntimeError("AUDIT_TABLE_FORMAT_UNEXPECTED")

    values = [int(x.replace(",", "")) for x in matches[0]]
    names = (
        "compared_minutes", "gold_events", "silver_events",
        "mismatched_minutes", "invalid_durations",
    )
    result = dict(zip(names, values))

    for key, value in result.items():
        print(key.upper(), "=", value, flush=True)

    passed = (
        result["compared_minutes"] == 24
        and result["gold_events"] == 100000
        and result["silver_events"] == 100000
        and result["mismatched_minutes"] == 0
        and result["invalid_durations"] == 0
    )

    result["status"] = "PASSED" if passed else "FAILED"
    AUDIT_PATH.write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )

    print("GOLD_V2_AUDIT =", result["status"])

    if not passed:
        raise RuntimeError("GOLD_V2_RECONCILIATION_FAILED")


def main():
    print("=== GOLD V2 ===", flush=True)
    preflight()

    # Nunca refazer o INSERT se já houve uma tentativa.
    # Isso evita reprocessamento não intencional.
    if MARKER.exists():
        print("PREVIOUS_SUBMISSION_DETECTED=True")
        print("INSPECT_GOLD_BEFORE_RETRY=True")
        raise SystemExit(1)

    sql = (
        ROOT / "sql/10_paimon_silver_to_gold_activity_1min_v2.sql"
    ).read_text(encoding="utf-8")

    MARKER.touch(exist_ok=False)

    print("SOURCE_EVENTS = 100000", flush=True)
    print("EXPECTED_WINDOWS = 24", flush=True)
    print("STAGE = materialization", flush=True)

    execute(sql, "GOLD_V2_MATERIALIZATION")

    print("GOLD_V2_MATERIALIZATION = COMPLETED", flush=True)
    print("STAGE = reconciliation", flush=True)

    audit()

    print("GOLD_V2_RESULT = PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("GOLD_V2_RESULT = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:180])
        print("DO_NOT_RERUN_WITHOUT_INSPECTION=True")
        raise SystemExit(1)
