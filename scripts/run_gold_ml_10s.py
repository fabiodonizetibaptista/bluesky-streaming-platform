"""Materializa e reconcilia a Gold experimental de ML.

Usa um warehouse isolado e uma única submissão protegida
por marcador. Não altera as camadas já validadas.
"""

import json
import re
import subprocess
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
SOURCE = (
    f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/silver"
)
GOLD = (
    f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/gold_ml_10s"
)

MARKER = RUN_DIR / "gold_ml_10s_submission_started"
AUDIT = RUN_DIR / "gold_ml_10s_audit.json"

CLIENT = [
    "docker", "compose",
    "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]


def execute(sql, label):
    completed = subprocess.run(
        CLIENT,
        cwd=ROOT,
        input=sql,
        text=True,
        capture_output=True,
        timeout=600,
    )

    output = (
        (completed.stdout or "")
        + "\n"
        + (completed.stderr or "")
    )

    if completed.returncode or "[ERROR]" in output:
        print(label + "_FAILED = True")
        print("EXIT_CODE =", completed.returncode)

        # Exibe somente trechos de erro, sem despejar
        # configurações ou informações de conexão.
        for line in output.splitlines():
            if "[ERROR]" in line or "Exception" in line:
                print("ERROR =", line[:220])

        raise RuntimeError(label + "_FAILED")

    return re.sub(
        r"\x1b\[[0-9;]*[A-Za-z]",
        "",
        output,
    )


def query_single_row(sql):
    output = execute(sql, "AUDIT_QUERY")

    matches = re.findall(
        r"^\|\s*(\d+)\s*\|\s*(\d+)\s*\|"
        r"\s*(\d+)\s*\|\s*(\d+)\s*\|"
        r"\s*(\d+)\s*\|\s*(\d+)\s*\|"
        r"\s*(\d+)\s*\|\s*(\d+)\s*\|"
        r"\s*(\d+)\s*\|$",
        output,
        flags=re.MULTILINE,
    )

    if len(matches) != 1:
        raise RuntimeError("UNEXPECTED_AUDIT_TABLE_FORMAT")

    return [int(value) for value in matches[0]]


def preflight():
    report = json.loads(
        (RUN_DIR / "status.json").read_text(encoding="utf-8")
    )

    if (
        report.get("run_id") != RUN_ID
        or report.get("stage") != "passed"
        or report.get("silver_uri") != SOURCE
        or report.get("reconciliation", {}).get("valid") != 100000
    ):
        raise RuntimeError("SOURCE_NOT_VALIDATED")

    with urlopen(
        "http://127.0.0.1:8081/jobs/overview",
        timeout=10,
    ) as response:
        jobs = json.load(response).get("jobs", [])

    if any(
        job.get("state") not in (
            "FINISHED", "FAILED", "CANCELED"
        )
        for job in jobs
    ):
        raise RuntimeError("ACTIVE_JOBS_PRESENT")

    if MARKER.exists():
        raise RuntimeError("PREVIOUS_SUBMISSION_PRESENT")


def audit():
    # A origem é agregada independentemente e comparada
    # com cada janela materializada.
    sql = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '4';

CREATE CATALOG silver_check WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{SOURCE}'
);

CREATE CATALOG gold_check WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{GOLD}'
);

CREATE TEMPORARY VIEW silver_windows AS
SELECT
    CAST(
        CONCAT(
            DATE_FORMAT(event_time_utc, 'yyyy-MM-dd HH:mm:'),
            LPAD(
                CAST(
                    CAST(
                        FLOOR(
                            EXTRACT(SECOND FROM event_time_utc) / 10
                        ) AS INT
                    ) * 10 AS STRING
                ),
                2,
                '0'
            )
        ) AS TIMESTAMP(6)
    ) AS window_start,
    operation_name
FROM silver_check.social.silver_valid_sink;

CREATE TEMPORARY VIEW expected AS
SELECT
    window_start,
    COUNT(*) AS total_events,
    SUM(CASE WHEN operation_name = 'create'
        THEN 1 ELSE 0 END) AS creates,
    SUM(CASE WHEN operation_name = 'update'
        THEN 1 ELSE 0 END) AS updates,
    SUM(CASE WHEN operation_name = 'delete'
        THEN 1 ELSE 0 END) AS deletes
FROM silver_windows
GROUP BY window_start;

SELECT
    COUNT(*) AS windows,
    COALESCE(SUM(g.total_events), 0) AS gold_events,
    COALESCE(SUM(e.total_events), 0) AS silver_events,
    COALESCE(SUM(
        CASE
            WHEN g.window_start IS NULL
              OR e.window_start IS NULL
              OR g.total_events <> e.total_events
              OR g.create_count <> e.creates
              OR g.update_count <> e.updates
              OR g.delete_count <> e.deletes
            THEN 1 ELSE 0
        END
    ), 0) AS mismatched_windows,
    COALESCE(SUM(
        CASE
            WHEN g.window_start IS NOT NULL
            AND (
                g.window_end IS NULL
                OR TIMESTAMPDIFF(
                    SECOND, g.window_start, g.window_end
                ) <> 10
            )
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_durations,
    COALESCE(SUM(
        CASE
            WHEN g.window_start IS NOT NULL
            AND MOD(
                EXTRACT(SECOND FROM g.window_start), 10
            ) <> 0
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_alignment,
    COALESCE(SUM(
        CASE
            WHEN g.window_start IS NOT NULL
            AND (
                g.distinct_source_sequences > g.total_events
                OR g.distinct_source_sequences IS NULL
            )
            THEN 1 ELSE 0
        END
    ), 0) AS invalid_distinct_counts,
    COALESCE(MAX(g.total_events), 0) AS max_events,
    COALESCE(MIN(g.total_events), 0) AS min_events
FROM expected e
FULL OUTER JOIN gold_check.social.gold_activity_10s g
    ON e.window_start = g.window_start;
"""

    values = query_single_row(sql)

    names = (
        "windows",
        "gold_events",
        "silver_events",
        "mismatched_windows",
        "invalid_durations",
        "invalid_alignment",
        "invalid_distinct_counts",
        "max_events",
        "min_events",
    )

    result = dict(zip(names, values))

    for key, value in result.items():
        print(key.upper(), "=", value, flush=True)

    passed = (
        result["windows"] == 142
        and result["gold_events"] == 100000
        and result["silver_events"] == 100000
        and result["mismatched_windows"] == 0
        and result["invalid_durations"] == 0
        and result["invalid_alignment"] == 0
        and result["invalid_distinct_counts"] == 0
        and result["max_events"] == 18005
        and result["min_events"] == 349
    )

    result["status"] = "PASSED" if passed else "FAILED"

    AUDIT.write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )

    print("GOLD_ML_10S_AUDIT =", result["status"])

    if not passed:
        raise RuntimeError("GOLD_ML_RECONCILIATION_FAILED")


def main():
    print("=== GOLD ML 10-SECOND MATERIALIZATION ===", flush=True)

    preflight()

    sql_path = (
        ROOT / "sql/11_paimon_silver_to_gold_activity_10s.sql"
    )

    # Marcador criado antes do primeiro INSERT.
    # Uma falha exige inspeção, nunca repetição automática.
    MARKER.touch(exist_ok=False)

    print("STAGE = materialization", flush=True)
    execute(sql_path.read_text(encoding="utf-8"), "MATERIALIZATION")

    print("MATERIALIZATION = COMPLETED", flush=True)
    print("STAGE = reconciliation", flush=True)

    audit()

    print("GOLD_ML_10S_RESULT = PASSED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("GOLD_ML_10S_RESULT = INCOMPLETE")
        print("ERROR_TYPE =", type(error).__name__)
        print("ERROR_CODE =", str(error)[:180])
        print("DO_NOT_RERUN_WITHOUT_INSPECTION = True")
        raise SystemExit(1)
