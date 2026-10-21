# End-to-End Streaming Pipeline

## Purpose

Demonstrate an integrated data engineering pipeline using
real public activity events from Bluesky Jetstream.

The execution covers:

1. Real-time event collection and publication to Kafka.
2. Stateful processing using Apache Flink.
3. Bronze, Silver, and Gold storage using Apache Paimon.
4. Event-time aggregations in ten-second Gold windows.
5. Anomaly detection using Isolation Forest.
6. Checkpoints, savepoints, and data reconciliation.

This is a portfolio proof of concept, not a production
service or a validated production anomaly detector.

## Data Flow

```text
Bluesky Jetstream
       |
       v
Python Collector
       |
       v
Apache Kafka
       |
       v
Apache Flink
       |
       v
Paimon Bronze
       |
       v
Paimon Silver --------> Quarantine
       |
       v
Flink Event-Time Windows
       |
       v
Paimon Gold (10 seconds)
       |
       v
Feature Engineering
       |
       v
Isolation Forest
       |
       v
Model, Metrics and Predictions
```

## Prerequisites

- Python 3.11 or newer
- Docker and Docker Compose
- Apache Kafka and Flink services configured
- Flink Kafka and Paimon connectors installed
- Network access to Bluesky Jetstream
- Project installed with its ML dependencies

Install the Python environment:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[ml,dev]"
```

Refer to the repository infrastructure scripts and
Compose files for service initialization.

Do not remove persistent volumes, checkpoints,
savepoints, or warehouses when preparing a run.

## Run the Pipeline

Execute from the repository root:

```bash
E2E_MIN_EVENTS=3000 \
E2E_MIN_DURATION_SECONDS=180 \
E2E_MAX_EVENTS=250000 \
E2E_MAX_DURATION_SECONDS=360 \
.venv/bin/python -u scripts/run_e2e_final.py
```

If your current shell user needs a Docker-enabled group
session, execute the command within that authorized
session instead of changing Docker socket permissions.

### Configuration

| Variable | Purpose |
|---|---|
| `E2E_MIN_EVENTS` | Minimum number of confirmed events |
| `E2E_MIN_DURATION_SECONDS` | Minimum collection duration |
| `E2E_MAX_EVENTS` | Maximum event safety limit |
| `E2E_MAX_DURATION_SECONDS` | Maximum collection duration |

These are execution parameters, not business rules.
The maximum limits must exceed the minimum requirements.

The executor generates a unique Run ID and creates
isolated Kafka and Paimon resources for each execution.

The Flink cluster must have the expected free capacity
and no competing active jobs for this executor.

## Execution Results

The following figures refer to one completed
end-to-end execution.

**Run ID:** `671370fce3cc`

| Metric | Observed value |
|---|---:|
| Real events collected | 8,345 |
| Bronze records | 8,345 |
| Silver valid records | 8,345 |
| Silver quarantine records | 0 |
| Finalized Gold windows | 17 |
| Events represented in finalized Gold windows | 7,554 |
| Silver events outside finalized Gold windows | 791 |
| Mismatched Gold windows | 0 |
| Late records dropped by the Gold aggregation | 0 |
| Saved Flink job states | 3 |
| ML training windows | 11 |
| ML evaluation windows | 6 |
| Anomaly alerts on the evaluation split | 0 |

**Result:** `FINAL_E2E_RESULT = PASSED`

**Process exit code:** `0`

### Interpreting Gold Results

Gold uses event-time windows.

Only windows finalized according to the watermark
are included in its final aggregated output.

The 791 Silver events outside finalized Gold windows
were accounted for during reconciliation. They must
not be reported as lost records.

The execution demonstrated zero mismatches in
the finalized Gold windows.

## Machine Learning

The model uses Isolation Forest with features derived
from the Gold time-window aggregates:

- Logarithmic event volume.
- Logarithmic volume ratio relative to the previous window.
- Share of delete operations.

The experiment uses a chronological train/evaluation
split and persists the trained model, evaluation
metrics, and predictions.

The observed split included 11 training windows and
6 evaluation windows.

This proves the functional ML integration. It is
not sufficient evidence of statistical generalization,
forecasting performance, or production-grade anomaly
detection.

Zero detected alerts does not mean zero anomalous
activity existed in the underlying event stream.

## Execution Artifacts

Each execution creates a dedicated local directory:

```text
runtime/flink/paimon/live_runs/<run_id>/
```

The directory contains an execution status journal,
rendered SQL files, Paimon warehouses, and ML outputs.

The ML output directory is:

```text
runtime/flink/paimon/live_runs/<run_id>/ml_final/
```

The experiment writes a model artifact, metrics JSON,
and prediction CSV.

All runtime artifacts remain local and are excluded
from version control.

## Reliability and Limitations

- Ingestion has at-least-once characteristics.
- End-to-end exactly-once delivery is not claimed.
- The environment is a single-node portfolio lab.
- A completed savepoint does not independently prove
  that restoration succeeds for every failure mode.
- The ML dataset is small and intended for integration
  validation.
- Streaming outputs depend on event-time and watermark
  progression.
- This project is not a production deployment.

## Automated Tests

```bash
.venv/bin/python -m pytest -q
```

At the time of this documented execution,
42 tests and 18 subtests passed.

These tests complement, but do not replace,
the live end-to-end experiment.
