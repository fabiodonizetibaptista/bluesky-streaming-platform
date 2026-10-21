"""Teste ao vivo: Bluesky -> Kafka -> Paimon Bronze -> Silver.

Usa o coletor original, com tópico e cursor isolados.
Não altera os SQLs oficiais e preserva todos os dados.
"""

import asyncio
import json
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka.admin import AdminClient, NewTopic


ROOT = Path(__file__).resolve().parents[1]
RUN_ID = uuid.uuid4().hex[:12]
RUN_DIR = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID
JOURNAL = RUN_DIR / "status.json"

TOPIC = f"bluesky.live.{RUN_ID}"
BRONZE = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/bronze"
SILVER = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/silver"
GOLD = f"file:///opt/flink/runtime/paimon/live_runs/{RUN_ID}/gold"

# Limites configuráveis do experimento E2E.
MIN_EVENTS = int(os.getenv("E2E_MIN_EVENTS", "3000"))
TARGET = int(os.getenv("E2E_MAX_EVENTS", "250000"))
MIN_DURATION_SECONDS = float(
    os.getenv("E2E_MIN_DURATION_SECONDS", "30")
)
MAX_DURATION_SECONDS = float(
    os.getenv("E2E_MAX_DURATION_SECONDS", "360")
)

if not (
    0 < MIN_EVENTS <= TARGET
    and 0 <= MIN_DURATION_SECONDS < MAX_DURATION_SECONDS
):
    raise ValueError("INVALID_E2E_CONFIGURATION")

SQL_CLIENT = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/sql-client.sh", "-f", "/dev/stdin",
]

STOP_COMMAND = [
    "docker", "compose", "-p", "financial-streaming-platform",
    "-f", "compose.flink.yaml",
    "exec", "-T", "flink-jobmanager",
    "/opt/flink/bin/flink", "stop",
    "--savepointPath", "file:///opt/flink/runtime/savepoints",
]

ACTIVE = {
    "CREATED", "INITIALIZING", "RUNNING", "RESTARTING",
    "RECONCILING", "FAILING", "CANCELLING", "SUSPENDED",
}

state = {
    "run_id": RUN_ID,
    "topic": TOPIC,
    "bronze_uri": BRONZE,
    "silver_uri": SILVER,
    "gold_uri": GOLD,
    "target": TARGET,
    "jobs": {},
    "savepoints": {},
    "stage": "initialized",
}


def api(endpoint):
    with urlopen(
        "http://127.0.0.1:8081" + endpoint, timeout=10
    ) as response:
        return json.load(response)


def persist(stage):
    state["stage"] = stage
    temp = JOURNAL.with_suffix(".tmp")
    if temp.exists():
        raise RuntimeError("JOURNAL_UPDATE_PENDING")
    temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp.replace(JOURNAL)
    print("STAGE =", stage, flush=True)


def sql_client(sql, purpose):
    try:
        result = subprocess.run(
            SQL_CLIENT, cwd=ROOT, input=sql,
            text=True, capture_output=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(purpose + "_TIMEOUT")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    if result.returncode or "[ERROR]" in output:
        # Não exibir payloads ou SQL contendo eventos.
        print(purpose + "_EXIT_CODE =", result.returncode)
        print(purpose + "_SQL_ERROR =", "[ERROR]" in output)
        raise RuntimeError(purpose + "_FAILED")

    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", output)


def replace_once(sql, original, replacement):
    if sql.count(original) != 1:
        raise RuntimeError("SQL_TEMPLATE_CONTRACT_CHANGED")
    return sql.replace(original, replacement, 1)


def build_sql(filename, substitutions):
    sql = (ROOT / "sql" / filename).read_text(encoding="utf-8")
    for original, replacement in substitutions:
        sql = replace_once(sql, original, replacement)
    return sql


def submit_job(label, sql, expected_name):
    old_ids = {
        item["jid"] for item in api("/jobs/overview").get("jobs", [])
    }

    persist(label + "_submission_started")
    sql_client(sql, label + "_SUBMISSION")

    deadline = time.monotonic() + 90

    while time.monotonic() < deadline:
        new = [
            item for item in api("/jobs/overview").get("jobs", [])
            if item["jid"] not in old_ids
        ]
        if len(new) > 1:
            raise RuntimeError(label + "_AMBIGUOUS_JOBS")
        if len(new) == 1:
            if new[0].get("name") != expected_name:
                raise RuntimeError(label + "_UNEXPECTED_JOB_NAME")
            job_id = new[0]["jid"]
            state["jobs"][label] = job_id
            persist(label + "_submitted")
            print(label + "_JOB_ID =", job_id)
            return job_id

        time.sleep(2)

    raise RuntimeError(label + "_JOB_NOT_IDENTIFIED")


def wait_checkpoint(job_id, label):
    deadline = time.monotonic() + 120

    while time.monotonic() < deadline:
        job = api(f"/jobs/{job_id}")
        status = job.get("state")

        if status in {"FAILED", "CANCELED", "FINISHED"}:
            raise RuntimeError(label + "_JOB_" + status)

        checkpoints = api(
            f"/jobs/{job_id}/checkpoints"
        ).get("counts", {}).get("completed", 0)

        if status == "RUNNING" and checkpoints >= 1:
            print(label + "_CHECKPOINTS =", checkpoints)
            return

        time.sleep(3)

    raise RuntimeError(label + "_CHECKPOINT_TIMEOUT")


def audit():
    query = f"""
SET 'parallelism.default' = '1';
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG bronze_audit WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{BRONZE}'
);

CREATE CATALOG silver_audit WITH (
 'type' = 'paimon',
 'metastore' = 'filesystem',
 'warehouse' = '{SILVER}'
);

SELECT
 'BRONZE' AS layer,
 COUNT(*) AS total,
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 )) AS unique_positions
FROM bronze_audit.social.bronze_events

UNION ALL

SELECT
 'VALID',
 COUNT(*),
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 ))
FROM silver_audit.social.silver_valid_sink

UNION ALL

SELECT
 'QUARANTINE',
 COUNT(*),
 COUNT(DISTINCT CONCAT(
   CAST(kafka_partition AS STRING), ':',
   CAST(kafka_offset AS STRING)
 ))
FROM silver_audit.social.silver_quarantine_sink;
"""

    output = sql_client(query, "LIVE_AUDIT")

    matches = re.findall(
        r"\|\s*(BRONZE|VALID|QUARANTINE)\s*"
        r"\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|",
        output,
    )

    if len(matches) != 3:
        raise RuntimeError("AUDIT_RESULT_FORMAT_UNEXPECTED")

    counts = {}
    for layer, total, unique in matches:
        if layer in counts:
            raise RuntimeError("AUDIT_DUPLICATED_LAYER")
        counts[layer] = (
            int(total.replace(",", "")),
            int(unique.replace(",", "")),
        )

    return counts


def reconcile(bronze_job, silver_job):
    expected = state["collector_confirmed"]
    deadline = time.monotonic() + 240

    while time.monotonic() < deadline:
        for label, job_id in (
            ("BRONZE", bronze_job),
            ("SILVER", silver_job),
        ):
            if api(f"/jobs/{job_id}").get("state") != "RUNNING":
                raise RuntimeError(label + "_NOT_RUNNING")

        counts = audit()
        bronze_total, bronze_unique = counts["BRONZE"]
        valid_total, valid_unique = counts["VALID"]
        quarantine_total, quarantine_unique = counts["QUARANTINE"]

        print(
            "BRONZE =", bronze_total,
            "VALID =", valid_total,
            "QUARANTINE =", quarantine_total,
            flush=True,
        )

        if (
            bronze_total > expected
            or valid_total + quarantine_total > expected
            or bronze_unique != bronze_total
            or valid_unique != valid_total
            or quarantine_unique != quarantine_total
        ):
            raise RuntimeError("LIVE_RECONCILIATION_INVALID")

        # A soma dos destinos precisa corresponder à Bronze,
        # mas só aprovamos quando todos os eventos chegarem.
        if (
            bronze_total == expected
            and valid_total + quarantine_total == expected
        ):
            state["reconciliation"] = {
                "bronze": bronze_total,
                "valid": valid_total,
                "quarantine": quarantine_total,
            }
            persist("reconciled")
            print("LIVE_RECONCILIATION = PASSED")
            return

        time.sleep(8)

    raise RuntimeError("LIVE_RECONCILIATION_TIMEOUT")


def stop_job(label, job_id):
    if api(f"/jobs/{job_id}").get("state") != "RUNNING":
        raise RuntimeError(label + "_NOT_RUNNING_BEFORE_STOP")

    persist(label + "_stop_requested")

    try:
        result = subprocess.run(
            STOP_COMMAND + [job_id],
            cwd=ROOT, text=True, capture_output=True, timeout=180,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(label + "_STOP_TIMEOUT")

    if result.returncode:
        raise RuntimeError(label + "_STOP_FAILED")

    output = (result.stdout or "") + "\n" + (result.stderr or "")

    match = re.search(
        r"file:/{1,3}opt/flink/runtime/savepoints/"
        r"(savepoint-[A-Za-z0-9-]+)",
        output,
    )

    if not match:
        raise RuntimeError(label + "_SAVEPOINT_NOT_IDENTIFIED")

    name = match.group(1)
    metadata = ROOT / "runtime/flink/savepoints" / name / "_metadata"

    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise RuntimeError(label + "_SAVEPOINT_METADATA_INVALID")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        status = api(f"/jobs/{job_id}").get("state")
        if status == "FINISHED":
            state["savepoints"][label] = name
            persist(label + "_stopped")
            print(label + "_STOP = PASSED")
            return
        if status in {"FAILED", "CANCELED"}:
            raise RuntimeError(label + "_UNEXPECTED_TERMINATION")
        time.sleep(2)

    raise RuntimeError(label + "_STOP_NOT_CONFIRMED")




def query_rows(sql, expected_columns, purpose):
    """Interpreta somente linhas de dados de saída TABLEAU."""
    output = sql_client(sql, purpose)
    rows = []

    for line in output.splitlines():
        line = line.strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue

        cells = [
            cell.strip()
            for cell in line[1:-1].split("|")
        ]

        if (
            len(cells) == expected_columns
            and re.fullmatch(r"[0-9,]+", cells[0])
        ):
            rows.append(cells)

    return rows


def audit_gold_final(job_id):
    """Reconcilia Gold finalizada com Silver, por janela."""
    from urllib.parse import urlencode

    sql = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG g_check WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = '{GOLD}'
);
CREATE CATALOG s_check WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = '{SILVER}'
);

WITH per_window AS (
 SELECT
   g.window_start,
   g.total_events AS expected_count,
   g.create_count AS expected_creates,
   g.update_count AS expected_updates,
   g.delete_count AS expected_deletes,
   COUNT(s.source_seq) AS actual_count,
   COALESCE(SUM(CASE WHEN s.operation_name = 'create'
        THEN 1 ELSE 0 END), 0) AS actual_creates,
   COALESCE(SUM(CASE WHEN s.operation_name = 'update'
        THEN 1 ELSE 0 END), 0) AS actual_updates,
   COALESCE(SUM(CASE WHEN s.operation_name = 'delete'
        THEN 1 ELSE 0 END), 0) AS actual_deletes
 FROM g_check.social.gold_activity_10s g
 LEFT JOIN s_check.social.silver_valid_sink s
   ON s.event_time_utc >= g.window_start
  AND s.event_time_utc < g.window_end
 GROUP BY g.window_start, g.total_events,
          g.create_count, g.update_count, g.delete_count
)
SELECT
 COUNT(*) AS windows,
 COALESCE(SUM(expected_count), 0) AS gold_events,
 COALESCE(SUM(actual_count), 0) AS silver_in_windows,
 COALESCE(SUM(CASE
   WHEN expected_count <> actual_count
     OR expected_creates <> actual_creates
     OR expected_updates <> actual_updates
     OR expected_deletes <> actual_deletes
   THEN 1 ELSE 0 END), 0) AS mismatches
FROM per_window;

SELECT COUNT(*) AS silver_outside
FROM s_check.social.silver_valid_sink s
WHERE NOT EXISTS (
 SELECT 1 FROM g_check.social.gold_activity_10s g
 WHERE s.event_time_utc >= g.window_start
   AND s.event_time_utc < g.window_end
);
"""

    output = sql_client(sql, "FINAL_GOLD_AUDIT")

    numeric = []
    for line in output.splitlines():
        line = line.strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue
        cells = [
            c.strip().replace(",", "")
            for c in line[1:-1].split("|")
        ]
        if cells and all(re.fullmatch(r"[0-9]+", c) for c in cells):
            numeric.append([int(c) for c in cells])

    summary = [row for row in numeric if len(row) == 4]
    outside_rows = [row for row in numeric if len(row) == 1]

    if len(summary) != 1 or len(outside_rows) != 1:
        raise RuntimeError("GOLD_AUDIT_FORMAT_UNEXPECTED")

    windows, gold_events, silver_in, mismatches = summary[0]
    outside = outside_rows[0][0]

    if (
        windows == 0
        or gold_events <= 0
        or gold_events != silver_in
        or mismatches != 0
        or gold_events + outside != state["reconciliation"]["valid"]
    ):
        raise RuntimeError("GOLD_RECONCILIATION_FAILED")

    late_count = 0
    late_found = False

    job = api(f"/jobs/{job_id}")

    for vertex in job.get("vertices", []):
        if "WindowAggregate" not in vertex.get("name", ""):
            continue

        vid = vertex["id"]
        tasks = api(f"/jobs/{job_id}/vertices/{vid}")["subtasks"]

        for task in tasks:
            endpoint = (
                f"/jobs/{job_id}/vertices/{vid}"
                f"/subtasks/{task['subtask']}/metrics"
            )

            names = [
                m["id"] for m in api(endpoint)
                if m["id"].endswith(".numLateRecordsDropped")
            ]

            for name in names:
                values = api(
                    endpoint + "?"
                    + urlencode({"get": name})
                )
                if len(values) != 1:
                    raise RuntimeError("LATE_METRIC_INVALID")
                late_count += int(values[0]["value"])
                late_found = True

    if not late_found:
        raise RuntimeError("LATE_METRIC_MISSING")

    # Descartes devem ser zero para a reconciliação integral
    # das janelas finalizadas nesta execução.
    if late_count != 0:
        raise RuntimeError("LATE_EVENTS_REQUIRE_REVIEW")

    state["gold_audit"] = {
        "windows": windows,
        "gold_events": gold_events,
        "silver_in_windows": silver_in,
        "silver_outside_windows": outside,
        "mismatched_windows": mismatches,
        "late_dropped": late_count,
    }
    persist("gold_audited")

    print("GOLD_WINDOWS =", windows, flush=True)
    print("GOLD_EVENTS =", gold_events, flush=True)
    print("SILVER_OUTSIDE_WINDOWS =", outside, flush=True)
    print("MISMATCHED_WINDOWS =", mismatches, flush=True)
    print("GOLD_LATE_DROPPED =", late_count, flush=True)
    print("GOLD_RECONCILIATION = PASSED", flush=True)


def run_ml_final():
    """Treina e avalia Isolation Forest com a Gold deste Run ID.

    Features:
      1. log1p(volume)
      2. log da razão contra a janela anterior
      3. participação de deletes

    Não reutiliza o modelo histórico nem inventa atraso
    de observação ausente no contrato da Gold atual.
    """
    import csv
    import math
    import tempfile
    from datetime import datetime, timedelta

    import numpy as np
    from sklearn.ensemble import IsolationForest
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler
    import joblib

    query = f"""
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG ml_gold WITH (
  'type' = 'paimon',
  'metastore' = 'filesystem',
  'warehouse' = '{GOLD}'
);

SELECT
  window_start,
  total_events,
  create_count,
  update_count,
  delete_count
FROM ml_gold.social.gold_activity_10s
ORDER BY window_start;
"""
    output = sql_client(query, "FINAL_ML_EXPORT")

    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not (line.startswith("|") and line.endswith("|")):
            continue

        cells = [c.strip() for c in line[1:-1].split("|")]

        if len(cells) != 5:
            continue

        # Valida o timestamp pelo parser de datas, não por
        # uma expressão regular com escapes frágeis.
        try:
            timestamp = datetime.fromisoformat(cells[0])
        except ValueError:
            continue

        # Uma linha com timestamp válido e valores inválidos
        # é erro de contrato, não um registro a ignorar.
        values = [int(c.replace(",", "")) for c in cells[1:]]
        rows.append((timestamp, *values))

    rows.sort(key=lambda row: row[0])

    if len(rows) != state["gold_audit"]["windows"]:
        raise RuntimeError("ML_WINDOW_COUNT_MISMATCH")

    features = []

    for i, row in enumerate(rows):
        timestamp, volume, creates, updates, deletes = row

        if volume <= 0 or creates + updates + deletes != volume:
            raise RuntimeError("ML_SOURCE_CONTRACT_INVALID")

        if i and timestamp - rows[i - 1][0] != timedelta(seconds=10):
            raise RuntimeError("ML_NONCONSECUTIVE_WINDOWS")

        previous = rows[i - 1][1] if i else volume

        features.append([
            math.log1p(volume),
            math.log((volume + 1) / (previous + 1)),
            deletes / volume,
        ])

    matrix = np.asarray(features, dtype=float)

    if not np.isfinite(matrix).all():
        raise RuntimeError("ML_NONFINITE_FEATURES")

    train_size = int(len(rows) * 0.7)
    test_size = len(rows) - train_size

    if train_size < 2 or test_size < 1:
        raise RuntimeError("ML_INSUFFICIENT_WINDOWS")

    model = Pipeline([
        ("scaler", RobustScaler()),
        ("detector", IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42,
        )),
    ])

    # Corte temporal: nenhuma observação futura participa
    # do treinamento. A validação usa apenas as posteriores.
    model.fit(matrix[:train_size])

    predictions = model.predict(matrix[train_size:])
    scores = model.decision_function(matrix[train_size:])

    if len(predictions) != test_size:
        raise RuntimeError("ML_PREDICTION_COUNT_INVALID")

    destination = RUN_DIR / "ml_final"

    if destination.exists():
        raise RuntimeError("ML_OUTPUT_ALREADY_EXISTS")

    destination.mkdir(parents=True, exist_ok=False)

    report = {
        "run_id": RUN_ID,
        "model": "IsolationForest",
        "feature_names": [
            "log_events",
            "log_ratio_previous",
            "delete_share",
        ],
        "source_windows": len(rows),
        "training_windows": train_size,
        "evaluation_windows": test_size,
        "evaluation_alerts": int(sum(predictions == -1)),
        "gold_events": state["gold_audit"]["gold_events"],
        "limitations": [
            "Sem rótulos de anomalia para medir precisão.",
            "Últimas janelas abertas não entram no treinamento.",
            "O treino é demonstrativo e específico deste Run ID.",
        ],
    }

    result_rows = [
        {
            "window_start": rows[train_size + i][0].isoformat(
                sep=" "
            ),
            "total_events": rows[train_size + i][1],
            "iforest_score": round(float(score), 6),
            "iforest_anomaly": int(prediction == -1),
        }
        for i, (score, prediction) in enumerate(
            zip(scores, predictions)
        )
    ]

    # Todos os arquivos são publicados dentro do Run ID.
    # Nenhum artefato do treinamento anterior é alterado.
    joblib.dump(model, destination / "model.joblib")

    with (destination / "predictions.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "window_start",
                "total_events",
                "iforest_score",
                "iforest_anomaly",
            ],
        )
        writer.writeheader()
        writer.writerows(result_rows)

    (destination / "metrics.json").write_text(
        json.dumps(report, indent=2) + chr(10),
        encoding="utf-8",
    )

    state["ml_result"] = report
    persist("ml_completed")

    print("ML_TRAIN_WINDOWS =", train_size, flush=True)
    print("ML_EVALUATION_WINDOWS =", test_size, flush=True)
    print("ML_ALERTS =", report["evaluation_alerts"], flush=True)
    print("ML_MODEL_SAVED = True", flush=True)
    print("ML_INFERENCE = PASSED", flush=True)



def main():
    print("=== BLUESKY LIVE STREAMING ===", flush=True)

    if Path.cwd().resolve() != ROOT:
        raise RuntimeError("RUN_FROM_REPOSITORY_ROOT")

    overview = api("/overview")
    jobs = api("/jobs/overview").get("jobs", [])

    if overview.get("slots-available") != 8:
        raise RuntimeError("EIGHT_FREE_SLOTS_REQUIRED")

    if any(item.get("state") in ACTIVE for item in jobs):
        raise RuntimeError("OTHER_ACTIVE_JOBS_PRESENT")

    RUN_DIR.mkdir(parents=True, exist_ok=False)
    persist("initialized")

    print("RUN_ID =", RUN_ID)
    print("TOPIC =", TOPIC)
    print("TARGET_EVENTS =", TARGET)

    # Um tópico exclusivo com uma partição simplifica a
    # reconciliação entre ACKs e offsets Kafka.
    persist("topic_creation_started")
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    admin.create_topics([
        NewTopic(
            TOPIC, num_partitions=1, replication_factor=1,
            config={"retention.ms": "86400000"},
        )
    ])[TOPIC].result(timeout=30)
    persist("topic_created")

    bronze_sql = build_sql(
        "07_kafka_to_paimon_bronze.sql",
        [
            ("'topic' = 'bluesky.posts.raw'", f"'topic' = '{TOPIC}'"),
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE,
            ),
        ],
    )

    silver_sql = build_sql(
        "08_paimon_bronze_to_silver.sql",
        [
            (
                "file:///opt/flink/runtime/paimon/platform_bronze_v1",
                BRONZE,
            ),
            (
                "file:///opt/flink/runtime/paimon/platform_silver_v1",
                SILVER,
            ),
        ],
    )

    (RUN_DIR / "bronze.sql").write_text(
        bronze_sql, encoding="utf-8"
    )
    (RUN_DIR / "silver.sql").write_text(
        silver_sql, encoding="utf-8"
    )

    bronze_job = submit_job(
        "BRONZE", bronze_sql,
        "insert-into_bronze_platform.social.bronze_events",
    )
    wait_checkpoint(bronze_job, "BRONZE")

    silver_job = submit_job(
        "SILVER", silver_sql,
        "insert-into_silver_platform.social.silver_valid_sink,"
        "silver_platform.social.silver_quarantine_sink",
    )
    wait_checkpoint(silver_job, "SILVER")

    # Confirma que ambos os jobs estão ativos antes de conectar
    # o coletor à fonte real de eventos.
    persist("both_jobs_streaming")
    print("BOTH_FLINK_JOBS_RUNNING = True")

    gold_sql = build_sql(
        "16_paimon_silver_to_gold_live.sql",
        [
            (
                "__SILVER_TABLE_PATH__",
                f"{SILVER}/social.db/silver_valid_sink",
            ),
            (
                "__GOLD_WAREHOUSE__",
                GOLD,
            ),
        ],
    )
    (RUN_DIR / "gold.sql").write_text(
        gold_sql, encoding="utf-8"
    )

    gold_job = submit_job(
        "GOLD", gold_sql,
        "insert-into_gold_streaming.social.gold_activity_10s",
    )
    wait_checkpoint(gold_job, "GOLD")
    persist("three_jobs_streaming")
    print("THREE_FLINK_JOBS_RUNNING = True", flush=True)

    # Apenas esta execução recebe o tópico e cursor isolados.
    # Não alteramos os arquivos de código do coletor.
    os.environ["KAFKA_TOPIC"] = TOPIC

    from bluesky_streaming import ingest

    ingest.CHECKPOINT_PATH = str(RUN_DIR / "live_cursor.json")
    ingest.MAX_EVENTS = TARGET

    print("=== CONNECTING REAL JETSTREAM ===", flush=True)
    persist("collector_started")
    confirmed = asyncio.run(
        ingest.ingest_events(
            min_events=MIN_EVENTS,
            min_duration_seconds=MIN_DURATION_SECONDS,
            max_duration_seconds=MAX_DURATION_SECONDS,
        )
    )

    state["collector_confirmed"] = confirmed
    persist("collector_completed")
    print("REAL_EVENTS_CONFIRMED =", confirmed)

    print("=== INDEPENDENT PAIMON RECONCILIATION ===", flush=True)
    reconcile(bronze_job, silver_job)

    # Fecha a Silver primeiro, permitindo à Gold ler os commits.
    stop_job("SILVER", silver_job)
    time.sleep(20)

    # A execução só poderá passar se a Gold reconciliar.
    audit_gold_final(gold_job)

    # Preserva o estado de streaming antes do ML batch.
    stop_job("GOLD", gold_job)
    stop_job("BRONZE", bronze_job)

    # Treinamento e inferência com as janelas desta execução.
    run_ml_final()

    persist("passed")
    print("LIVE_PIPELINE_RESULT = PASSED")
    print("LIVE_RUN_ID =", RUN_ID)
    print("BRONZE_ROWS =", state["reconciliation"]["bronze"])
    print("SILVER_VALID_ROWS =", state["reconciliation"]["valid"])
    print("QUARANTINE_ROWS =", state["reconciliation"]["quarantine"])
    print("THREE_JOBS_STOPPED_WITH_SAVEPOINT = True")
    print("FINAL_E2E_RESULT = PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("LIVE_PIPELINE_RESULT = INCOMPLETE")
        print("ERROR_TYPE =", type(exc).__name__)
        print("ERROR_CODE =", str(exc)[:180])
        print("LIVE_RUN_ID =", RUN_ID)
        print("INSPECT_JOURNAL_BEFORE_ANY_RETRY = True")
        raise SystemExit(1)
