"""Validação de inferência independente do treinamento.

Carrega exclusivamente o modelo produzido neste projeto.
Não reentreina, não acessa Kafka e não altera Paimon.

ATENÇÃO: joblib usa pickle. Nunca carregar modelos de
fontes externas não confiáveis.
"""

import csv
import json
import math
from datetime import datetime
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

def load_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))

def main():
    metrics = json.loads(
        (EXPERIMENT / "metrics.json").read_text()
    )

    if metrics.get("run_id") != RUN_ID:
        raise RuntimeError("MODEL_RUN_MISMATCH")

    if metrics.get("train_windows") != 99:
        raise RuntimeError("UNEXPECTED_TRAIN_CONTRACT")

    source = load_csv(ML_DIR / "activity_10s.csv")
    reference = load_csv(EXPERIMENT / "predictions.csv")

    if len(source) != 142 or len(reference) != 43:
        raise RuntimeError("DATASET_SIZE_MISMATCH")

    # Reproduz a transformação do experimento original.
    # A janela anterior é necessária para calcular a
    # variação; não pode ser descartada na inferência.
    features = []

    for i, row in enumerate(source):
        volume = int(row["total_events"])
        previous = (
            int(source[i - 1]["total_events"])
            if i else volume
        )
        deletes = int(row["delete_count"])
        delay = float(
            row["avg_observation_delay_ms"] or 0
        )

        features.append([
            math.log1p(volume),
            math.log((volume + 1) / (previous + 1)),
            deletes / volume,
            math.log1p(max(0, delay)),
        ])

    cutoff = datetime.fromisoformat(metrics["train_last"])

    selected_indices = [
        i for i, row in enumerate(source)
        if datetime.fromisoformat(row["window_start"]) > cutoff
    ]

    if len(selected_indices) != 43:
        raise RuntimeError("INFERENCE_WINDOW_MISMATCH")

    # O modelo deve ser exclusivamente aquele gerado
    # pelo treinamento local previamente validado.
    model = joblib.load(EXPERIMENT / "model.joblib")
    matrix = np.asarray(
        [features[i] for i in selected_indices],
        dtype=float,
    )

    predictions = model.predict(matrix)
    scores = model.decision_function(matrix)

    # Normaliza timestamps antes da comparação.
    # O CSV Gold contém microssegundos, enquanto o arquivo
    # de predições foi salvo sem a fração quando ela é zero.
    # Comparar datetime evita divergências de formatação.
    reference_by_time = {
        datetime.fromisoformat(row["window_start"]): row
        for row in reference
    }

    mismatches = 0
    score_max_difference = 0.0
    alerts = 0

    for index, prediction, score in zip(
        selected_indices, predictions, scores
    ):
        timestamp = datetime.fromisoformat(
            source[index]["window_start"]
        )
        expected = reference_by_time.get(timestamp)

        if expected is None:
            raise RuntimeError("REFERENCE_WINDOW_MISSING")

        flag = int(prediction == -1)
        alerts += flag

        if flag != int(expected["iforest_anomaly"]):
            mismatches += 1

        difference = abs(
            round(float(score), 6)
            - float(expected["iforest_score"])
        )
        score_max_difference = max(
            score_max_difference, difference
        )

    print("=== INDEPENDENT MODEL INFERENCE ===")
    print("MODEL_RUN =", RUN_ID)
    print("TRAINING_EXECUTED = False")
    print("INFERENCE_WINDOWS =", len(selected_indices))
    print("IFOREST_ALERTS =", alerts)
    print("PREDICTION_MISMATCHES =", mismatches)
    print(
        "MAX_SCORE_DIFFERENCE =",
        round(score_max_difference, 9),
    )

    passed = (
        mismatches == 0
        and score_max_difference <= 0.000001
        and alerts == metrics["iforest_test_flags"]
    )

    print(
        "INDEPENDENT_INFERENCE =",
        "PASSED" if passed else "FAILED"
    )

    if not passed:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
