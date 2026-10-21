"""Exporta métricas agregadas Gold 10s para experimentos de ML.

Somente leitura no Paimon. Não exporta payloads ou dados
pessoais. Valida totais antes de publicar o CSV local.
"""

import csv
import json
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
OUTPUT_DIR = RUN_DIR / "ml"
CSV_PATH = OUTPUT_DIR / "activity_10s.csv"
SUMMARY_PATH = OUTPUT_DIR / "dataset_summary.json"

WAREHOUSE = (
    "file:///opt/flink/runtime/paimon/"
    f"live_runs/{RUN_ID}/gold_ml_10s"
)

COLUMNS = [
    "window_start",
    "window_end",
    "total_events",
    "distinct_source_sequences",
    "create_count",
    "update_count",
    "delete_count",
    "avg_observation_delay_ms",
    "max_observation_delay_ms",
]

SQL = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'parallelism.default' = '4';

CREATE CATALOG ml_export WITH (
    'type' = 'paimon',
    'metastore' = 'filesystem',
    'warehouse' = '{WAREHOUSE}'
);

SELECT
    window_start,
    window_end,
    total_events,
    distinct_source_sequences,
    create_count,
    update_count,
    delete_count,
    avg_observation_delay_ms,
    max_observation_delay_ms
FROM ml_export.social.gold_activity_10s
ORDER BY window_start;
"""

COMMAND = [
    "docker", "compose",
    "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh",
    "-f", "/dev/stdin",
]


def parse_timestamp(value):
    return datetime.fromisoformat(value)


def main():
    audit_path = RUN_DIR / "gold_ml_10s_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))

    if audit.get("status") != "PASSED":
        raise RuntimeError("GOLD_AUDIT_NOT_PASSED")

    result = subprocess.run(
        COMMAND,
        cwd=ROOT,
        input=SQL,
        capture_output=True,
        text=True,
        timeout=300,
    )

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        raise RuntimeError("GOLD_EXPORT_QUERY_FAILED")

    output = re.sub(
        r"\x1b\[[0-9;]*[A-Za-z]", "", output
    )

    rows = []

    for line in output.splitlines():
        line = line.strip()

        if not line.startswith("|"):
            continue

        cells = [cell.strip() for cell in line.strip("|").split("|")]

        if len(cells) != len(COLUMNS):
            continue

        if not re.match(r"^\d{4}-\d{2}-\d{2} ", cells[0]):
            continue

        row = dict(zip(COLUMNS, cells))

        for column in (
            "total_events",
            "distinct_source_sequences",
            "create_count",
            "update_count",
            "delete_count",
        ):
            row[column] = int(row[column].replace(",", ""))

        for column in (
            "avg_observation_delay_ms",
            "max_observation_delay_ms",
        ):
            row[column] = (
                None if row[column].upper() == "NULL"
                else float(row[column])
            )

        rows.append(row)

    rows.sort(key=lambda item: item["window_start"])

    # As 142 observações são verificadas antes de gerar
    # qualquer arquivo consumido por treinamento.
    if len(rows) != 142:
        raise RuntimeError(
            f"UNEXPECTED_WINDOW_COUNT:{len(rows)}"
        )

    if sum(row["total_events"] for row in rows) != 100000:
        raise RuntimeError("EVENT_COUNT_MISMATCH")

    for index, row in enumerate(rows):
        start = parse_timestamp(row["window_start"])
        end = parse_timestamp(row["window_end"])

        if end - start != timedelta(seconds=10):
            raise RuntimeError("INVALID_WINDOW_DURATION")

        if start.second % 10 != 0 or start.microsecond:
            raise RuntimeError("INVALID_WINDOW_ALIGNMENT")

        if (
            row["create_count"]
            + row["update_count"]
            + row["delete_count"]
            != row["total_events"]
        ):
            raise RuntimeError("INVALID_OPERATION_TOTAL")

        if index and start - parse_timestamp(
            rows[index - 1]["window_start"]
        ) != timedelta(seconds=10):
            raise RuntimeError("MISSING_OR_OVERLAPPING_WINDOW")

    # Não confundir pico de 18.005 eventos com anomalia
    # comprovada. Apenas identificamos a janela para análise.
    peak = max(rows, key=lambda row: row["total_events"])

    summary = {
        "run_id": RUN_ID,
        "source": "Paimon Gold ML 10s",
        "windows": len(rows),
        "events": sum(row["total_events"] for row in rows),
        "first_window": rows[0]["window_start"],
        "last_window": rows[-1]["window_start"],
        "min_events": min(row["total_events"] for row in rows),
        "max_events": peak["total_events"],
        "peak_window": peak["window_start"],
        "peak_create_count": peak["create_count"],
        "peak_update_count": peak["update_count"],
        "peak_delete_count": peak["delete_count"],
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    SUMMARY_PATH.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    print("=== GOLD ML DATASET ===")

    for key, value in summary.items():
        print(key.upper(), "=", value)

    print("DATASET_EXPORT = PASSED")
    print("DATASET_PATH =", CSV_PATH.relative_to(ROOT))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("DATASET_EXPORT = FAILED")
        print("ERROR_TYPE =", type(error).__name__)
        print("ERROR_CODE =", str(error)[:180])
        raise SystemExit(1)
