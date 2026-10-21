# Machine Learning — Activity Anomaly Detection

## Purpose

The Machine Learning component demonstrates how
streaming data can support an analytical workflow
that extends beyond ingestion and transformation.

The project uses Gold-layer activity aggregates
to train an Isolation Forest model and generate
anomaly predictions.

This is a functional ML integration experiment,
not a production anomaly detection service.

## Architecture

```text
Paimon Gold
    |
    | Finalized 10-second windows
    v
Feature Engineering
    |
    | Activity volume
    | Relative volume change
    | Delete operation share
    v
Chronological Dataset
    |
    +--------> Training Set
    |              |
    |              v
    |         RobustScaler
    |              |
    |              v
    |        Isolation Forest
    |              |
    +--------> Evaluation Set
                   |
                   v
               Inference
                   |
                   v
          Metrics and Predictions
```

## Data Source

The model consumes finalized activity windows
from the Paimon Gold table:

`gold_activity_10s`

Each window provides:

| Field | Description |
|---|---|
| `window_start` | Window start timestamp |
| `window_end` | Window end timestamp |
| `total_events` | Total events in the window |
| `create_count` | Number of create operations |
| `update_count` | Number of update operations |
| `delete_count` | Number of delete operations |

These aggregates describe activity patterns
rather than individual user content.

## Feature Engineering

The validated E2E implementation uses three
features for each Gold window.

### 1. Logarithmic Event Volume

```text
log1p(total_events)
```

The logarithmic transformation reduces
the influence of extreme activity volumes.

### 2. Relative Volume Change

```text
log(
    (current_window_events + 1)
    /
    (previous_window_events + 1)
)
```

This feature represents the proportional change
in event volume relative to the preceding
observation.

The additional constant avoids division by zero.

### 3. Delete Operation Share

```text
delete_count / total_events
```

This feature measures the proportion of delete
operations within the activity window.

The model uses only aggregate activity features.
It does not analyze individual post content.

## Model

The experiment uses Isolation Forest, an
unsupervised anomaly detection algorithm.

The E2E implementation combines:

- RobustScaler for feature normalization.
- IsolationForest for anomaly detection.
- A fixed random seed for reproducibility.
- Model persistence using joblib.

Isolation Forest identifies observations
that differ from the patterns represented
in the training data.

Its predictions should not automatically
be interpreted as security incidents,
fraud, or confirmed abnormal behavior.

## Training and Evaluation

The model uses a chronological split.

The first portion of the Gold dataset is used
for training, and the remaining portion is
reserved for evaluation.

This structure preserves temporal ordering
instead of randomly mixing earlier and later
observations.

The feature describing relative volume change
uses information from the preceding window,
which is available at the time of observation.

## Validated E2E Experiment

**Run ID:** `671370fce3cc`

| Metric | Result |
|---|---:|
| Gold windows available | 17 |
| Training windows | 11 |
| Evaluation windows | 6 |
| Detected alerts | 0 |
| Model persisted | Yes |
| Inference completed | Yes |
| End-to-end execution | PASSED |

The experiment demonstrated that the
data engineering pipeline can generate
features, train a model and perform inference
using data collected during the same run.

### Interpretation

Zero alerts means that the fitted model
did not flag any of the six evaluation
observations under its configured threshold.

It does not establish that anomalous
activity was absent from the source.

The evaluation dataset is too small
to support strong claims about model quality.

## Output Artifacts

Each E2E execution writes ML outputs
under its isolated Run ID directory.

```text
runtime/flink/paimon/live_runs/<run_id>/ml_final/
    model.joblib
    metrics.json
    predictions.csv
```

### Model Artifact

`model.joblib` contains the trained
machine learning pipeline.

### Metrics

`metrics.json` records experiment-level
results and relevant model metadata.

### Predictions

`predictions.csv` contains inference
results for the evaluation observations.

Runtime artifacts are excluded from Git.

## Reproducibility

The project declares optional ML dependencies
in `pyproject.toml`.

Install them using:

```bash
.venv/bin/python -m pip install -e ".[ml,dev]"
```

Execute the integrated experiment using
`scripts/run_e2e_final.py`.

The runner creates separate storage paths
for each execution.

Model results may differ between executions
because the live input stream changes.

## Limitations

- The model is trained on a small dataset
  in the validated experiment.
- No labeled ground-truth anomalies are available.
- Precision, recall and detection accuracy
  cannot be established from the observed run.
- The feature set represents aggregate
  activity, not event content or user behavior.
- The experiment does not implement a
  continuously deployed inference service.
- The model is not intended for production
  incident detection or automated decisions.

## Related Documentation

- [Architecture](../architecture/README.md)
- [Processing](../processing/README.md)
- [End-to-End Execution](../e2e/README.md)
