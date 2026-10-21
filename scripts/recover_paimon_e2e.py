"""Continua uma execução E2E interrompida após submissão da Bronze.

Não cria tópicos, não publica eventos e não reinicia jobs existentes.
Uso exclusivo para uma execução previamente identificada e validada.
"""

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "a72fab2e7411"
RUN_DIR = ROOT / "runtime/flink/paimon/e2e_runs" / RUN_ID
JOURNAL = RUN_DIR / "status.json"

spec = importlib.util.spec_from_file_location(
    "paimon_e2e_runner", ROOT / "scripts/run_paimon_e2e.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def main():
    print("=== RECOVER EXISTING E2E RUN ===", flush=True)

    if not JOURNAL.is_file():
        raise RuntimeError("ORIGINAL_JOURNAL_MISSING")

    record = json.loads(JOURNAL.read_text(encoding="utf-8"))

    expected_topic = f"bluesky.e2e.{RUN_ID}"
    expected_bronze = (
        f"file:///opt/flink/runtime/paimon/e2e_runs/{RUN_ID}/bronze"
    )
    expected_silver = (
        f"file:///opt/flink/runtime/paimon/e2e_runs/{RUN_ID}/silver"
    )

    if (
        record.get("run_id") != RUN_ID
        or record.get("topic") != expected_topic
        or record.get("bronze_uri") != expected_bronze
        or record.get("silver_uri") != expected_silver
    ):
        raise RuntimeError("EXPERIMENT_IDENTITY_MISMATCH")

    if record.get("stage") != "BRONZE_submitted":
        raise RuntimeError(
            "UNEXPECTED_STAGE_DO_NOT_REPEAT_RECOVERY"
        )

    bronze_job = record.get("jobs", {}).get("BRONZE")

    if bronze_job != "ab3b18a97edaa7a1069987b4f40510da":
        raise RuntimeError("BRONZE_JOB_ID_MISMATCH")

    # Reutiliza as funções de submissão, reconciliação e savepoint
    # do runner versionado, sem chamar seu main() novamente.
    runner.RUN_ID = RUN_ID
    runner.RUN_DIR = RUN_DIR
    runner.JOURNAL = JOURNAL
    runner.TOPIC = expected_topic
    runner.BRONZE_URI = expected_bronze
    runner.SILVER_URI = expected_silver
    runner.state = record

    jobs = runner.api("/jobs/overview").get("jobs", [])
    active = [
        job for job in jobs
        if job.get("state") in runner.ACTIVE
    ]

    if len(active) != 1 or active[0].get("jid") != bronze_job:
        raise RuntimeError("UNEXPECTED_ACTIVE_JOBS")

    details = runner.api(f"/jobs/{bronze_job}")

    if (
        details.get("state") != "RUNNING"
        or details.get("name") != runner.JOB_BRONZE
    ):
        raise RuntimeError("BRONZE_JOB_NOT_EXPECTED_RUNNING_JOB")

    print("ORIGINAL_RUN_ID =", RUN_ID)
    print("ORIGINAL_BRONZE_JOB =", bronze_job)
    print("NO_NEW_EVENTS_PUBLISHED = True")

    print("=== RECONCILE BRONZE ===", flush=True)

    prefix = runner.catalog_prefix()

    bronze_read = prefix + """
SELECT 'BRONZE', CAST(kafka_partition AS STRING),
       CAST(kafka_offset AS STRING)
FROM bronze_platform.social.bronze_events
ORDER BY kafka_offset;
"""

    runner.wait_rows(
        bronze_read,
        [("BRONZE", "0", str(i)) for i in range(5)],
        "BRONZE",
    )
    runner.persist("bronze_data_validated")

    print("=== STOP ORIGINAL BRONZE WITH SAVEPOINT ===", flush=True)
    runner.stop_with_savepoint(bronze_job, "BRONZE")

    print("=== SUBMIT SILVER ===", flush=True)

    silver_sql = runner.template_sql(
        "08_paimon_bronze_to_silver.sql",
        [
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                expected_bronze,
            ),
            (
                "file:///opt/flink/runtime/paimon/platform_silver_v1",
                expected_silver,
            ),
        ],
    )

    silver_job = runner.submit(
        silver_sql, runner.JOB_SILVER, "SILVER"
    )
    runner.wait_checkpoint(silver_job, "SILVER")

    print("=== RECONCILE SILVER AND QUARANTINE ===", flush=True)

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

    runner.wait_rows(
        reconciliation_sql, expected, "SILVER"
    )
    runner.persist("silver_data_validated")

    print("=== STOP SILVER WITH SAVEPOINT ===", flush=True)
    runner.stop_with_savepoint(silver_job, "SILVER")

    runner.persist("passed")

    print("=== FINAL RESULT ===")
    print("E2E_RESULT = PASSED")
    print("BRONZE_RECORDS = 5")
    print("SILVER_VALID_RECORDS = 1")
    print("QUARANTINE_RECORDS = 4")
    print("BOTH_JOBS_STOPPED_WITH_SAVEPOINT = True")
    print("NO_NEW_TOPIC_CREATED = True")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("E2E_RECOVERY = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:200])
        print("RUN_ID =", RUN_ID)
        print("DO_NOT_REPEAT_WITHOUT_INSPECTING_JOURNAL = True")
        raise SystemExit(1)
