"""Valida, sem modificar dados, o teste isolado Kafka -> Paimon Bronze."""

import json
import re
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
MARKER = ROOT / "runtime/flink/paimon/bronze_integration_active.json"

CLIENT = [
    "docker", "compose",
    "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]


def flink_api(path):
    with urlopen("http://127.0.0.1:8081" + path, timeout=10) as response:
        return json.load(response)


def query_bronze(warehouse):
    sql = f"""
SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'table.local-time-zone' = 'UTC';

CREATE CATALOG bronze_platform WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{warehouse}'
);

USE CATALOG bronze_platform;
USE social;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT CONCAT(
        CAST(kafka_partition AS STRING), ':',
        CAST(kafka_offset AS STRING)
    )) AS distinct_positions,
    COALESCE(SUM(CASE WHEN
        payload IS NULL OR kafka_partition IS NULL
        OR kafka_offset IS NULL OR kafka_timestamp IS NULL
        THEN 1 ELSE 0 END), 0) AS invalid_metadata,
    COALESCE(SUM(CASE WHEN
        kafka_partition <> 0 OR kafka_offset < 0
        THEN 1 ELSE 0 END), 0) AS invalid_coordinates,
    COALESCE(SUM(CASE WHEN
        JSON_VALUE(payload, '$.source')
            IS DISTINCT FROM 'bluesky_jetstream_v2'
        OR TRY_CAST(JSON_VALUE(payload, '$.source_seq') AS BIGINT)
            IS DISTINCT FROM 100001
        OR JSON_VALUE(payload, '$.collection')
            IS DISTINCT FROM 'app.bsky.feed.post'
        OR JSON_VALUE(payload, '$.operation')
            IS DISTINCT FROM 'create'
        OR JSON_VALUE(payload, '$.event_time')
            IS DISTINCT FROM '2026-10-20T20:00:00Z'
        OR JSON_VALUE(payload, '$.witnessed_at')
            IS DISTINCT FROM '2026-10-20T20:00:01Z'
        THEN 1 ELSE 0 END), 0) AS invalid_contract
FROM bronze_events;
"""
    result = subprocess.run(
        CLIENT,
        input=sql,
        text=True,
        capture_output=True,
        cwd=ROOT,
        timeout=180,
    )
    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        print("QUERY_DIAGNOSTIC =", output[-1200:])
        raise RuntimeError("BRONZE_QUERY_FAILED")

    # O SQL Client imprime uma tabela ASCII. Capturamos somente
    # a linha de resultados, sem exibir payloads ou dados individuais.
    matches = []
    for line in output.splitlines():
        if re.fullmatch(r"\|\s*\d+\s*(?:\|\s*\d+\s*){4}\|", line):
            matches.append(tuple(
                int(value.strip())
                for value in line.strip().strip("|").split("|")
            ))

    if len(matches) != 1:
        print("RESULT_ROW_COUNT =", len(matches))
        raise RuntimeError("AGGREGATE_RESULT_NOT_RECOGNIZED")

    return matches[0]


def main():
    print("=== VERIFY EXPERIMENT IDENTITY ===", flush=True)

    if not MARKER.is_file():
        raise RuntimeError("EXPERIMENT_MARKER_MISSING")

    record = json.loads(MARKER.read_text(encoding="utf-8"))

    if record.get("stage") not in {
        "checkpoint_validated", "data_validated"
    }:
        raise RuntimeError("UNEXPECTED_EXPERIMENT_STAGE")

    run_id = record["run_id"]
    job_id = record["job_id"]
    warehouse = record["warehouse"]

    if not re.fullmatch(r"[0-9a-f]{12}", run_id):
        raise RuntimeError("INVALID_RUN_ID")

    expected_warehouse = (
        "file:///opt/flink/runtime/paimon/bronze_integration/"
        + run_id
    )
    if warehouse != expected_warehouse:
        raise RuntimeError("WAREHOUSE_IDENTITY_MISMATCH")

    job = flink_api(f"/jobs/{job_id}")

    if job.get("state") != "RUNNING":
        raise RuntimeError("BRONZE_JOB_NOT_RUNNING")

    if job.get("name") != "insert-into_bronze_platform.social.bronze_events":
        raise RuntimeError("BRONZE_JOB_NAME_MISMATCH")

    print("RUN_ID =", run_id)
    print("JOB_ID =", job_id)
    print("JOB_STATE = RUNNING")

    print("=== VERIFY PERSISTED SNAPSHOT ===", flush=True)

    deadline = time.monotonic() + 90
    validated = False

    while time.monotonic() < deadline:
        values = query_bronze(warehouse)

        total, distinct, invalid_metadata, invalid_coords, invalid_contract = values

        print("TOTAL_ROWS =", total)
        print("DISTINCT_KAFKA_POSITIONS =", distinct)
        print("INVALID_METADATA =", invalid_metadata)
        print("INVALID_COORDINATES =", invalid_coords)
        print("INVALID_CONTRACT =", invalid_contract)

        if values == (1, 1, 0, 0, 0):
            validated = True
            break

        if total > 1 or any(values[2:]):
            break

        time.sleep(5)

    checkpoints = flink_api(
        f"/jobs/{job_id}/checkpoints"
    ).get("counts", {}).get("completed", 0)

    still_running = flink_api(f"/jobs/{job_id}").get("state") == "RUNNING"

    print("COMPLETED_CHECKPOINTS =", checkpoints)
    print("CONSUMER_STILL_RUNNING =", still_running)

    passed = validated and checkpoints >= 1 and still_running

    print(
        "KAFKA_TO_PAIMON_BRONZE =",
        "PASSED" if passed else "FAILED",
    )

    if not passed:
        raise RuntimeError("BRONZE_INTEGRATION_NOT_VALIDATED")

    record["stage"] = "data_validated"
    temporary = MARKER.with_suffix(".json.tmp")
    if temporary.exists():
        raise RuntimeError("PENDING_MARKER_UPDATE")
    temporary.write_text(
        json.dumps(record, indent=2), encoding="utf-8"
    )
    temporary.replace(MARKER)


if __name__ == "__main__":
    main()
