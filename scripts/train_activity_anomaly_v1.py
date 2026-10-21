"""PoC de detecção de anomalias em atividade Bluesky.

Fonte: métricas Gold agregadas em 10 segundos.
Treinamento: primeiros 70% dos intervalos.
Avaliação: últimos 30% dos intervalos.

Nenhuma informação futura entra nas features de uma janela.
Os resultados são experimentais, não métricas de produção.
"""

import csv
import json
import math
import statistics
from datetime import datetime, timedelta
from pathlib import Path

import joblib
import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "e0602ddcb53d"
ML_DIR = (
    ROOT / "runtime/flink/paimon/live_runs"
    / RUN_ID / "ml"
)

DATASET = ML_DIR / "activity_10s.csv"
AUDIT = ML_DIR.parent / "gold_ml_10s_audit.json"
OUTPUT = ML_DIR / "experiment_v1"

FEATURE_NAMES = [
    "log_events",
    "log_ratio_previous",
    "delete_share",
    "log_observation_delay",
]


def read_dataset():
    if json.loads(AUDIT.read_text())["status"] != "PASSED":
        raise RuntimeError("GOLD_AUDIT_NOT_PASSED")

    with DATASET.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if len(rows) != 142:
        raise RuntimeError("UNEXPECTED_DATASET_SIZE")

    observations = []

    for item in rows:
        observations.append({
            "timestamp": datetime.fromisoformat(
                item["window_start"]
            ),
            "events": int(item["total_events"]),
            "creates": int(item["create_count"]),
            "updates": int(item["update_count"]),
            "deletes": int(item["delete_count"]),
            "delay": float(
                item["avg_observation_delay_ms"]
                or 0.0
            ),
        })

    observations.sort(key=lambda item: item["timestamp"])

    if sum(r["events"] for r in observations) != 100000:
        raise RuntimeError("DATASET_RECONCILIATION_FAILED")

    for i, row in enumerate(observations):
        if row["events"] <= 0:
            raise RuntimeError("NONPOSITIVE_VOLUME")

        if row["creates"] + row["updates"] + row["deletes"] != row["events"]:
            raise RuntimeError("OPERATION_MISMATCH")

        if i and row["timestamp"] - observations[i - 1]["timestamp"] != timedelta(seconds=10):
            raise RuntimeError("MISSING_TIME_WINDOW")

    return observations


def build_features(rows):
    """Utiliza a janela atual e, no máximo, a anterior."""

    features = []

    for i, row in enumerate(rows):
        current = row["events"]
        previous = rows[i - 1]["events"] if i else current

        features.append([
            math.log1p(current),
            math.log((current + 1) / (previous + 1)),
            row["deletes"] / current,
            math.log1p(max(0.0, row["delay"])),
        ])

    matrix = np.asarray(features, dtype=float)

    if not np.isfinite(matrix).all():
        raise RuntimeError("NONFINITE_FEATURES")

    return matrix


def main():
    rows = read_dataset()
    features = build_features(rows)

    split = int(len(rows) * 0.70)

    train_x = features[:split]
    test_x = features[split:]
    train_rows = rows[:split]
    test_rows = rows[split:]

    if not train_rows[-1]["timestamp"] < test_rows[0]["timestamp"]:
        raise RuntimeError("TEMPORAL_LEAKAGE")

    # Baseline: limite aprendido somente com os volumes
    # do treinamento. Não usa dados futuros para calibrar.
    train_volumes = [r["events"] for r in train_rows]
    median = statistics.median(train_volumes)
    mad = statistics.median(
        abs(value - median) for value in train_volumes
    )

    # Fator 1.4826 converte MAD para uma escala comparável
    # ao desvio padrão sob uma distribuição normal.
    robust_scale = max(1.0, 1.4826 * mad)
    baseline_threshold = 6.0

    def baseline_score(value):
        return abs(value - median) / robust_scale

    # O RobustScaler aprende mediana e IQR exclusivamente
    # no treinamento. O Isolation Forest é reprodutível.
    model = Pipeline([
        ("scaler", RobustScaler()),
        ("detector", IsolationForest(
            n_estimators=200,
            max_samples="auto",
            contamination=0.05,
            random_state=42,
            n_jobs=2,
        )),
    ])

    model.fit(train_x)

    # Score menor que zero indica classe anômala no IF.
    if_scores = model.decision_function(test_x)
    if_predictions = model.predict(test_x)

    results = []

    for row, score, prediction in zip(
        test_rows, if_scores, if_predictions
    ):
        baseline = baseline_score(row["events"])

        results.append({
            "window_start": row["timestamp"].isoformat(
                sep=" "
            ),
            "total_events": row["events"],
            "create_count": row["creates"],
            "update_count": row["updates"],
            "delete_count": row["deletes"],
            "avg_observation_delay_ms": row["delay"],
            "baseline_score": round(baseline, 5),
            "baseline_anomaly": int(
                baseline > baseline_threshold
            ),
            "iforest_score": round(float(score), 6),
            "iforest_anomaly": int(prediction == -1),
        })

    # Teste controlado: cria um volume extremo sintético,
    # sem modificar o CSV original, Gold ou modelo treinado.
    # Usa uma janela normal do conjunto de avaliação.
    synthetic = features[split].copy()
    original_volume = test_rows[0]["events"]
    synthetic_volume = max(20000, original_volume * 25)

    synthetic[0] = math.log1p(synthetic_volume)
    synthetic[1] = math.log(
        (synthetic_volume + 1)
        / (rows[split - 1]["events"] + 1)
    )

    synthetic_if_score = float(
        model.decision_function([synthetic])[0]
    )
    synthetic_if_anomaly = bool(
        model.predict([synthetic])[0] == -1
    )
    synthetic_baseline_anomaly = bool(
        baseline_score(synthetic_volume)
        > baseline_threshold
    )

    peak = max(results, key=lambda r: r["total_events"])

    summary = {
        "run_id": RUN_ID,
        "experiment": "activity_anomaly_v1",
        "sklearn_version": sklearn.__version__,
        "feature_names": FEATURE_NAMES,
        "train_windows": len(train_rows),
        "test_windows": len(test_rows),
        "train_first": train_rows[0]["timestamp"].isoformat(),
        "train_last": train_rows[-1]["timestamp"].isoformat(),
        "test_first": test_rows[0]["timestamp"].isoformat(),
        "test_last": test_rows[-1]["timestamp"].isoformat(),
        "baseline_median": median,
        "baseline_mad": mad,
        "baseline_threshold": baseline_threshold,
        "baseline_test_flags": sum(
            r["baseline_anomaly"] for r in results
        ),
        "iforest_test_flags": sum(
            r["iforest_anomaly"] for r in results
        ),
        "peak_window": peak["window_start"],
        "peak_events": peak["total_events"],
        "peak_baseline_flag": peak["baseline_anomaly"],
        "peak_iforest_flag": peak["iforest_anomaly"],
        "peak_iforest_score": peak["iforest_score"],
        "synthetic_volume": synthetic_volume,
        "synthetic_baseline_flag": synthetic_baseline_anomaly,
        "synthetic_iforest_flag": synthetic_if_anomaly,
        "synthetic_iforest_score": synthetic_if_score,
        "limitations": [
            "Apenas uma sessão de aproximadamente 24 minutos.",
            "Não existem rótulos reais para medir precisão ou recall.",
            "A avaliação sintética mede resposta a um cenário artificial.",
            "Picos podem refletir replay, reconexão ou efeitos de coleta.",
            "O modelo foi treinado em batch, não online.",
        ],
    }

    OUTPUT.mkdir(parents=True, exist_ok=True)

    predictions_path = OUTPUT / "predictions.csv"

    with predictions_path.open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(results[0].keys())
        )
        writer.writeheader()
        writer.writerows(results)

    (OUTPUT / "metrics.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    # Arquivo joblib deve ser carregado somente de fontes
    # confiáveis, pois utiliza serialização baseada em pickle.
    joblib.dump(model, OUTPUT / "model.joblib")

    print("=== MACHINE LEARNING EXPERIMENT V1 ===")
    print("TRAIN_WINDOWS =", summary["train_windows"])
    print("TEST_WINDOWS =", summary["test_windows"])
    print("BASELINE_MEDIAN =", median)
    print("BASELINE_MAD =", mad)
    print("BASELINE_TEST_FLAGS =", summary["baseline_test_flags"])
    print("IFOREST_TEST_FLAGS =", summary["iforest_test_flags"])
    print("PEAK_WINDOW =", summary["peak_window"])
    print("PEAK_EVENTS =", summary["peak_events"])
    print("PEAK_BASELINE_FLAG =", summary["peak_baseline_flag"])
    print("PEAK_IFOREST_FLAG =", summary["peak_iforest_flag"])
    print("PEAK_IFOREST_SCORE =", summary["peak_iforest_score"])
    print("SYNTHETIC_VOLUME =", synthetic_volume)
    print("SYNTHETIC_BASELINE_FLAG =", synthetic_baseline_anomaly)
    print("SYNTHETIC_IFOREST_FLAG =", synthetic_if_anomaly)
    print("MODEL_TRAINING = COMPLETED")
    print("ARTIFACT_DIR =", OUTPUT.relative_to(ROOT))


if __name__ == "__main__":
    main()
