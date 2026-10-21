"""Audita a Gold de atividade por minuto contra a Silver validada.

Somente leitura. Não ressubmete INSERTs nem altera warehouses.
"""

import json
import re
import subprocess
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
JOURNAL = RUN_DIR / "status.json"

def main():
    report = json.loads(JOURNAL.read_text(encoding="utf-8"))
    expected = report.get("reconciliation", {}).get("valid")

    if report.get("stage") != "passed" or expected != 100000:
        raise RuntimeError("SOURCE_RUN_NOT_VALIDATED")

    warehouse = (
        f"file:///opt/flink/runtime/paimon/live_runs/"
        f"{RUN_ID}/gold"
    )

    sql = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG gold_audit WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{warehouse}'
);

SELECT
    COUNT(*) AS windows,
    COALESCE(SUM(total_events), 0) AS events,
    COALESCE(SUM(create_count), 0) AS creates,
    COALESCE(SUM(update_count), 0) AS updates,
    COALESCE(SUM(delete_count), 0) AS deletes,
    COUNT(*) - COUNT(DISTINCT window_start) AS repeated_windows,
    COALESCE(SUM(
        CASE
            WHEN distinct_source_sequences > total_events
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_distinct_counts,
    COALESCE(SUM(
        CASE
            WHEN window_end IS NULL
              OR TIMESTAMPDIFF(
                    SECOND, window_start, window_end
                 ) <> 60
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_window_durations
FROM gold_audit.social.gold_activity_1min;
"""

    command = [
        "docker", "compose",
        "-p", "financial-streaming-platform",
        "-f", "compose.flink.yaml",
        "exec", "-T", "flink-jobmanager",
        "/opt/flink/bin/sql-client.sh",
        "-f", "/dev/stdin",
    ]

    print("=== GOLD V1 AUDIT ===", flush=True)
    print("SOURCE_RUN =", RUN_ID, flush=True)

    result = subprocess.run(
        command,
        cwd=ROOT,
        input=sql,
        text=True,
        capture_output=True,
        timeout=300,
    )

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        lines = [
            line[:220] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]
        print("SQL_ERROR_LINES =", lines[-5:])
        raise RuntimeError("GOLD_QUERY_FAILED")

    output = re.sub(
        r"\x1b\[[0-9;]*[A-Za-z]", "", output
    )

    matches = re.findall(
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*\|",
        output,
    )

    if len(matches) != 1:
        print("MATCHING_TABLE_ROWS =", len(matches))
        print("SQL_OUTPUT_LINES =", len(output.splitlines()))
        raise RuntimeError("AUDIT_RESULT_FORMAT_UNEXPECTED")

    values = [
        int(value.replace(",", ""))
        for value in matches[0]
    ]

    names = (
        "WINDOWS",
        "TOTAL_EVENTS",
        "CREATE_EVENTS",
        "UPDATE_EVENTS",
        "DELETE_EVENTS",
        "REPEATED_WINDOWS",
        "INVALID_DISTINCT_COUNTS",
        "INVALID_WINDOW_DURATIONS",
    )

    metrics = dict(zip(names, values))

    for name, value in metrics.items():
        print(name, "=", value)

    checks = {
        "HAS_WINDOWS": metrics["WINDOWS"] > 0,
        "RECONCILES_WITH_SILVER":
            metrics["TOTAL_EVENTS"] == expected,
        "OPERATIONS_RECONCILE":
            metrics["CREATE_EVENTS"]
            + metrics["UPDATE_EVENTS"]
            + metrics["DELETE_EVENTS"]
            == metrics["TOTAL_EVENTS"],
        "UNIQUE_WINDOWS":
            metrics["REPEATED_WINDOWS"] == 0,
        "DISTINCT_COUNTS_VALID":
            metrics["INVALID_DISTINCT_COUNTS"] == 0,
        "WINDOW_DURATION_VALID":
            metrics["INVALID_WINDOW_DURATIONS"] == 0,
    }

    for name, passed in checks.items():
        print(name, "=", "PASSED" if passed else "FAILED")

    if not all(checks.values()):
        raise RuntimeError("GOLD_RECONCILIATION_FAILED")

    print("GOLD_V1_AUDIT = PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("GOLD_V1_AUDIT = INCOMPLETE")
        print("ERROR_TYPE =", type(error).__name__)
        print("ERROR_CODE =", str(error)[:180])
        raise SystemExit(1)
