"""Reconcilia a Bronze 10K e encerra o job com savepoint.

Não republica mensagens, não exclui dados e não reenvia SQLs.
"""

import json
import re
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "6b9d03c76ba9"
JOB_ID = "0eb213f7f922fe406558da50bf20f826"
JOURNAL = ROOT / "runtime/benchmarks/bronze" / RUN_ID / "status.json"
SAVEPOINT_ROOT = ROOT / "runtime/flink/savepoints"

CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml", "exec", "-T",
    "flink-jobmanager", "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]


def api(path):
    with urlopen("http://127.0.0.1:8081" + path, timeout=10) as response:
        return json.load(response)


def save(record, stage):
    record["stage"] = stage
    temporary = JOURNAL.with_suffix(".tmp")

    if temporary.exists():
        raise RuntimeError("PENDING_JOURNAL_UPDATE")

    temporary.write_text(json.dumps(record, indent=2), encoding="utf-8")
    temporary.replace(JOURNAL)
    print("STAGE =", stage, flush=True)


def query(sql):
    try:
        result = subprocess.run(
            CLIENT, cwd=ROOT, input=sql,
            capture_output=True, text=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("QUERY_TIMEOUT")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode != 0 or "[ERROR]" in output:
        errors = [
            line[:250] for line in output.splitlines()
            if "[ERROR]" in line or "Exception" in line
        ]
        print("SQL_ERROR_LINES =", errors[-4:])
        raise RuntimeError("PAIMON_QUERY_FAILED")

    # Quatro campos numéricos:
    # total, posições distintas, coordenadas inválidas,
    # quantidade de partições efetivamente presentes.
    clean_output = re.sub(
        r"\x1b\[[0-9;]*[A-Za-z]", "", output
    )

    matches = re.findall(
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*"
        r"\|\s*([0-9,]+)\s*\|",
        clean_output,
    )

    if len(matches) != 1:
        print("RESULT_TABLE_ROW_COUNT =", len(matches))
        print("SQL_OUTPUT_LINE_COUNT =", len(clean_output.splitlines()))
        print("SQL_OUTPUT_HAS_TABLEAU_ROWS =", "|" in clean_output)
        raise RuntimeError("UNEXPECTED_QUERY_RESULT_FORMAT")

    return tuple(
        int(value.replace(",", "")) for value in matches[0]
    )


def main():
    print("=== BRONZE BENCHMARK RECONCILIATION ===", flush=True)

    record = json.loads(JOURNAL.read_text(encoding="utf-8"))

    if record.get("run_id") != RUN_ID:
        raise RuntimeError("RUN_ID_MISMATCH")

    if record.get("job_id") != JOB_ID:
        raise RuntimeError("JOB_ID_MISMATCH")

    if record.get("stage") != "running":
        raise RuntimeError("RUN_ALREADY_ADVANCED_DO_NOT_REPEAT")

    source = json.loads(
        (
            ROOT / "runtime/benchmarks"
            / f"{record['source_run']}.json"
        ).read_text(encoding="utf-8")
    )

    if source.get("status") != "PASSED":
        raise RuntimeError("SOURCE_REPORT_NOT_APPROVED")

    expected_count = source["events"]
    expected_partitions = source["partitions"]
    warehouse = record["warehouse"]

    expected_warehouse = (
        f"file:///opt/flink/runtime/benchmarks/bronze/"
        f"{RUN_ID}/warehouse"
    )

    if warehouse != expected_warehouse:
        raise RuntimeError("WAREHOUSE_IDENTITY_MISMATCH")

    job = api(f"/jobs/{JOB_ID}")

    if (
        job.get("state") != "RUNNING"
        or job.get("name")
        != "insert-into_bronze_platform.social.bronze_events"
    ):
        raise RuntimeError("UNEXPECTED_BRONZE_JOB")

    sql = f"""
SET 'parallelism.default' = '4';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG bronze_audit WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = '{warehouse}'
);

SELECT
  COUNT(*),
  COUNT(DISTINCT CONCAT(
    CAST(kafka_partition AS STRING),
    ':',
    CAST(kafka_offset AS STRING)
  )),
  SUM(
    CASE
      WHEN kafka_partition IS NULL OR kafka_offset IS NULL
      THEN 1
      ELSE 0
    END
  ),
  COUNT(DISTINCT kafka_partition)
FROM bronze_audit.social.bronze_events;
"""

    deadline = time.monotonic() + 180
    final_counts = None

    while time.monotonic() < deadline:
        if api(f"/jobs/{JOB_ID}").get("state") != "RUNNING":
            raise RuntimeError("BRONZE_JOB_NOT_RUNNING")

        total, unique, invalid, partitions = query(sql)
        print(
            "OBSERVED_TOTAL =", total,
            "UNIQUE_POSITIONS =", unique,
            "INVALID_COORDINATES =", invalid,
            "PARTITIONS =", partitions,
            flush=True,
        )

        if total > expected_count or unique > expected_count:
            raise RuntimeError("MORE_ROWS_THAN_KAFKA_SOURCE")

        if (
            total == expected_count
            and unique == expected_count
            and invalid == 0
            and partitions == expected_partitions
        ):
            final_counts = (total, unique, invalid, partitions)
            break

        time.sleep(5)

    if final_counts is None:
        raise RuntimeError("BRONZE_RECONCILIATION_TIMEOUT")

    print("BRONZE_RECONCILIATION = PASSED")
    record["verified_rows"] = final_counts[0]
    record["verified_unique_positions"] = final_counts[1]
    record["verified_partitions"] = final_counts[3]
    save(record, "reconciled")

    # A partir deste registro, uma falha não permite repetir
    # automaticamente a solicitação de savepoint.
    save(record, "stop_requested")

    command = [
        "docker", "compose", "-p", "financial-streaming-platform",
        "-f", "compose.flink.yaml", "exec", "-T",
        "flink-jobmanager", "/opt/flink/bin/flink", "stop",
        "--savepointPath",
        "file:///opt/flink/runtime/savepoints",
        JOB_ID,
    ]

    try:
        result = subprocess.run(
            command, cwd=ROOT,
            capture_output=True, text=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("STOP_TIMEOUT_INSPECT_BEFORE_RETRY")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode:
        raise RuntimeError("STOP_FAILED_INSPECT_BEFORE_RETRY")

    match = re.search(
        r"file:/{1,3}opt/flink/runtime/savepoints/"
        r"(savepoint-[A-Za-z0-9-]+)",
        output,
    )

    if not match:
        raise RuntimeError("SAVEPOINT_NOT_IDENTIFIED")

    name = match.group(1)
    metadata = SAVEPOINT_ROOT / name / "_metadata"

    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise RuntimeError("SAVEPOINT_METADATA_INVALID")

    deadline = time.monotonic() + 90

    while time.monotonic() < deadline:
        status = api(f"/jobs/{JOB_ID}").get("state")
        if status == "FINISHED":
            break
        if status in {"FAILED", "CANCELED"}:
            raise RuntimeError("UNEXPECTED_FINAL_STATE")
        time.sleep(2)
    else:
        raise RuntimeError("JOB_STOP_NOT_CONFIRMED")

    record["savepoint_name"] = name
    record["bronze_reconciliation"] = "PASSED"
    save(record, "stopped_with_savepoint")

    print("SAVEPOINT_NAME =", name)
    print("FINAL_JOB_STATE = FINISHED")
    print("BRONZE_BENCHMARK_FINALIZED = PASSED")
    print("BRONZE_ROWS =", final_counts[0])
    print("WAREHOUSE =", warehouse)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("BRONZE_BENCHMARK_FINALIZED = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:200])
        print("DO_NOT_RERUN_WITHOUT_INSPECTING_JOURNAL = True")
        raise SystemExit(1)
