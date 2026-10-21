"""Inferência incremental idempotente — experimento histórico.

Consome somente janelas posteriores ao treinamento.
Persiste resultados por chave temporal, sem sobrescrever
previsões existentes e sem reentreinar o modelo.

Nesta versão, a fonte é o CSV Gold exportado e validado.
"""

import argparse
import csv
import json
import math
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import joblib
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
ML = ROOT / "runtime/flink/paimon/live_runs" / RUN_ID / "ml"
EXPERIMENT = ML / "experiment_v1"
DEST = ML / "incremental_v1"
OUTPUT = DEST / "predictions.csv"

FIELDS = [
    "window_start",
    "total_events",
    "iforest_score",
    "iforest_anomaly",
]


def timestamp(value):
    return datetime.fromisoformat(value)


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def atomic_write(rows):
    """Publica o arquivo completo com substituição atômica."""
    DEST.mkdir(parents=True, exist_ok=True)

    fd, temporary = tempfile.mkstemp(
        prefix=".predictions_",
        suffix=".csv",
        dir=DEST,
        text=True,
    )

    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, OUTPUT)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-new", type=int, default=20
    )
    args = parser.parse_args()

    if args.max_new < 1:
        raise SystemExit("MAX_NEW_MUST_BE_POSITIVE")

    metrics = json.loads(
        (EXPERIMENT / "metrics.json").read_text(encoding="utf-8")
    )

    if metrics["run_id"] != RUN_ID:
        raise SystemExit("MODEL_RUN_MISMATCH")

    source = read_csv(ML / "activity_10s.csv")
    existing = read_csv(OUTPUT)

    if len(source) != 142:
        raise SystemExit("SOURCE_CONTRACT_INVALID")

    if sum(int(r["total_events"]) for r in source) != 100000:
        raise SystemExit("SOURCE_RECONCILIATION_FAILED")

    # Normaliza a chave temporal: diferenças de precisão
    # textual não podem causar inferências duplicadas.
    source.sort(key=lambda r: timestamp(r["window_start"]))
    cutoff = timestamp(metrics["train_last"])

    eligible = [
        i for i, row in enumerate(source)
        if timestamp(row["window_start"]) > cutoff
    ]

    if len(eligible) != 43:
        raise SystemExit("EVALUATION_CONTRACT_INVALID")

    completed = {}

    for row in existing:
        key = timestamp(row["window_start"])

        if key in completed:
            raise SystemExit("DUPLICATE_OUTPUT_WINDOW")

        completed[key] = row

    eligible_keys = {
        timestamp(source[i]["window_start"]) for i in eligible
    }

    if not set(completed).issubset(eligible_keys):
        raise SystemExit("OUTPUT_CONTAINS_UNKNOWN_WINDOWS")

    pending = [
        i for i in eligible
        if timestamp(source[i]["window_start"]) not in completed
    ]

    selected = pending[:args.max_new]

    print("=== INCREMENTAL INFERENCE V1 ===", flush=True)
    print("ELIGIBLE_WINDOWS =", len(eligible))
    print("ALREADY_PROCESSED =", len(completed))
    print("NEW_WINDOWS =", len(selected))

    if selected:
        # Somente o modelo produzido localmente pelo projeto
        # pode ser carregado. joblib/pickle não é seguro
        # para artefatos provenientes de terceiros.
        model = joblib.load(EXPERIMENT / "model.joblib")

        features = []

        for i in selected:
            row = source[i]
            previous = source[i - 1]

            current_volume = int(row["total_events"])
            previous_volume = int(previous["total_events"])
            delete_count = int(row["delete_count"])
            delay = float(
                row["avg_observation_delay_ms"] or 0
            )

            features.append([
                math.log1p(current_volume),
                math.log(
                    (current_volume + 1)
                    / (previous_volume + 1)
                ),
                delete_count / current_volume,
                math.log1p(max(0.0, delay)),
            ])

        matrix = np.asarray(features, dtype=float)

        if not np.isfinite(matrix).all():
            raise SystemExit("INVALID_MODEL_FEATURES")

        scores = model.decision_function(matrix)
        predictions = model.predict(matrix)

        for i, score, prediction in zip(
            selected, scores, predictions
        ):
            source_row = source[i]
            key = timestamp(source_row["window_start"])

            completed[key] = {
                "window_start": key.isoformat(sep=" "),
                "total_events": int(
                    source_row["total_events"]
                ),
                "iforest_score": round(float(score), 6),
                "iforest_anomaly": int(prediction == -1),
            }

        ordered = [
            completed[key] for key in sorted(completed)
        ]

        atomic_write(ordered)

    records = read_csv(OUTPUT)

    if len(records) != len(completed):
        raise SystemExit("PERSISTENCE_COUNT_MISMATCH")

    alerts = sum(
        int(row["iforest_anomaly"]) for row in records
    )

    print("PERSISTED_WINDOWS =", len(records))
    print("PERSISTED_ALERTS =", alerts)
    print("REMAINING_WINDOWS =", 43 - len(records))
    print("INCREMENTAL_INFERENCE = PASSED")


if __name__ == "__main__":
    main()
