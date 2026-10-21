"""Inferência incremental para datasets Gold crescentes.

O treinamento não é executado aqui.

A entrada deve conter janelas consecutivas, finalizadas
e ordenáveis por timestamp. O processo não considera uma
janela aberta como finalizada por conta própria.

Idempotência: execuções sequenciais, por chave temporal.
Não oferece bloqueio para escritores concorrentes.
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

ML_DIR = (
    ROOT / "runtime/flink/paimon/live_runs"
    / RUN_ID / "ml"
)

EXPERIMENT = ML_DIR / "experiment_v1"
DEFAULT_SOURCE = ML_DIR / "activity_10s.csv"
DEFAULT_OUTPUT = (
    ML_DIR / "incremental_v2" / "predictions.csv"
)

FIELDS = [
    "window_start",
    "total_events",
    "iforest_score",
    "iforest_anomaly",
]


def read_csv(path):
    if not path.exists():
        return []

    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def timestamp(value):
    return datetime.fromisoformat(value)


def atomic_write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp = tempfile.mkstemp(
        prefix=".predictions_",
        suffix=".csv",
        dir=path.parent,
        text=True,
    )

    try:
        with os.fdopen(
            fd, "w", newline="", encoding="utf-8"
        ) as handle:
            writer = csv.DictWriter(
                handle, fieldnames=FIELDS
            )
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temp, path)

    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def validate_source(rows):
    if len(rows) < 100:
        raise ValueError("INSUFFICIENT_HISTORY")

    rows.sort(
        key=lambda row: timestamp(row["window_start"])
    )

    seen = set()

    for index, row in enumerate(rows):
        start = timestamp(row["window_start"])

        if start in seen:
            raise ValueError("DUPLICATE_SOURCE_WINDOW")

        seen.add(start)

        if start.second % 10 or start.microsecond:
            raise ValueError("INVALID_TIME_ALIGNMENT")

        volume = int(row["total_events"])
        creates = int(row["create_count"])
        updates = int(row["update_count"])
        deletes = int(row["delete_count"])

        if volume <= 0:
            raise ValueError("NONPOSITIVE_VOLUME")

        if creates + updates + deletes != volume:
            raise ValueError("INVALID_OPERATION_COUNTS")

        if index:
            previous = timestamp(
                rows[index - 1]["window_start"]
            )
            if start - previous != timedelta(seconds=10):
                raise ValueError("NONCONSECUTIVE_WINDOWS")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )
    parser.add_argument(
        "--max-new",
        type=int,
        default=100,
    )

    args = parser.parse_args()

    if args.max_new < 1:
        raise ValueError("INVALID_MAX_NEW")

    # Restringe saídas ao runtime do projeto. Não aceita
    # sobrescrever arquivos arbitrários por parâmetro.
    runtime_root = (ROOT / "runtime").resolve()
    output_path = args.output.resolve()

    if not output_path.is_relative_to(runtime_root):
        raise ValueError("OUTPUT_OUTSIDE_RUNTIME")

    metrics = json.loads(
        (EXPERIMENT / "metrics.json").read_text(
            encoding="utf-8"
        )
    )

    if metrics.get("run_id") != RUN_ID:
        raise ValueError("MODEL_RUN_MISMATCH")

    if metrics.get("feature_names") != [
        "log_events",
        "log_ratio_previous",
        "delete_share",
        "log_observation_delay",
    ]:
        raise ValueError("FEATURE_CONTRACT_MISMATCH")

    source = read_csv(args.source)

    if not source:
        raise ValueError("EMPTY_SOURCE")

    validate_source(source)

    existing = read_csv(output_path)
    completed = {}

    for row in existing:
        key = timestamp(row["window_start"])

        if key in completed:
            raise ValueError("DUPLICATE_OUTPUT_KEY")

        completed[key] = row

    cutoff = timestamp(metrics["train_last"])

    eligible = [
        i for i, row in enumerate(source)
        if timestamp(row["window_start"]) > cutoff
    ]

    eligible_keys = {
        timestamp(source[i]["window_start"])
        for i in eligible
    }

    if not set(completed).issubset(eligible_keys):
        raise ValueError("UNKNOWN_OUTPUT_KEY")

    pending = [
        i for i in eligible
        if timestamp(source[i]["window_start"])
        not in completed
    ]

    selected = pending[:args.max_new]

    if selected:
        # Somente carregar o artefato gerado localmente.
        # Nunca usar joblib.load com modelos não confiáveis.
        model = joblib.load(
            EXPERIMENT / "model.joblib"
        )

        matrix = []

        for index in selected:
            row = source[index]
            previous = source[index - 1]

            volume = int(row["total_events"])
            prior_volume = int(previous["total_events"])
            deletes = int(row["delete_count"])
            delay = float(
                row["avg_observation_delay_ms"] or 0
            )

            matrix.append([
                math.log1p(volume),
                math.log(
                    (volume + 1) / (prior_volume + 1)
                ),
                deletes / volume,
                math.log1p(max(0.0, delay)),
            ])

        matrix = np.asarray(matrix, dtype=float)

        if not np.isfinite(matrix).all():
            raise ValueError("NONFINITE_FEATURES")

        scores = model.decision_function(matrix)
        predictions = model.predict(matrix)

        for index, score, prediction in zip(
            selected, scores, predictions
        ):
            row = source[index]
            key = timestamp(row["window_start"])

            completed[key] = {
                "window_start": key.isoformat(sep=" "),
                "total_events": int(row["total_events"]),
                "iforest_score": round(float(score), 6),
                "iforest_anomaly": int(prediction == -1),
            }

        atomic_write(
            output_path,
            [completed[key] for key in sorted(completed)],
        )

    persisted = read_csv(output_path)
    alerts = sum(
        int(row["iforest_anomaly"])
        for row in persisted
    )

    print("=== INCREMENTAL INFERENCE V2 ===")
    print("SOURCE_WINDOWS =", len(source))
    print("ELIGIBLE_WINDOWS =", len(eligible))
    print("PREVIOUSLY_PROCESSED =", len(existing))
    print("NEW_WINDOWS =", len(selected))
    print("PERSISTED_WINDOWS =", len(persisted))
    print("ALERTS =", alerts)
    print("REMAINING =", len(pending) - len(selected))
    print("INFERENCE_V2 = PASSED")


if __name__ == "__main__":
    main()
