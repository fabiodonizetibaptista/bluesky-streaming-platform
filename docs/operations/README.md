# Operations — Local Environment and Execution

## Purpose

This guide describes how to prepare and operate the
Bluesky Streaming Platform in a local engineering lab.

The project demonstrates streaming data engineering
using Docker Compose, Apache Kafka, Apache Flink,
Apache Paimon, Prometheus, Grafana and Python.

It is not a production deployment guide.

## Prerequisites

- Linux environment with Docker Engine.
- Docker Compose plugin.
- Python 3.11 or newer.
- Network access to Bluesky Jetstream.
- Sufficient CPU, memory and disk capacity.
- Permission to operate the local Docker daemon.

The validated environment used an Oracle Linux VM.

## Repository Setup

Clone the project:

```bash
git clone \
  https://github.com/fabiodonizetibaptista/bluesky-streaming-platform.git

cd bluesky-streaming-platform
```

Create a Python environment and install the package
with development and ML dependencies:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[ml,dev]"
```

An editable installation allows changes under
`src/` to be reflected without reinstalling the
package after each source-code modification.

If the repository directory is moved, reinstall
the editable package from the new directory.

## Infrastructure

The environment uses separate Compose definitions:

| File | Component |
|---|---|
| `compose.yaml` | Apache Kafka |
| `compose.flink.yaml` | Apache Flink |
| `compose.monitoring.yaml` | Prometheus and Grafana |

### Compose Project Names

The existing Compose project names are retained
for compatibility with persisted infrastructure.

| Component | Compose project |
|---|---|
| Kafka | `financial-streaming` |
| Flink | `financial-streaming-platform` |
| Monitoring | `financial-streaming-monitoring` |

These internal identifiers are independent of the
public repository name.

Avoid changing project names without reviewing
container, network and volume mappings.

## Connector Installation

Install the required connectors using the existing
repository scripts:

```bash
bash scripts/install_flink_connector.sh
bash scripts/install_paimon_connectors.sh
```

Connector binaries are excluded from version control.

The installation scripts use pinned artifacts and
integrity verification.

## Start the Infrastructure

Start Kafka:

```bash
docker compose -p financial-streaming \
  -f compose.yaml up -d
```

Start Flink:

```bash
docker compose -p financial-streaming-platform \
  -f compose.flink.yaml up -d
```

Start monitoring:

```bash
docker compose -p financial-streaming-monitoring \
  -f compose.monitoring.yaml up -d
```

Always review the Compose configuration and
persistent volumes before changing infrastructure.

Do not use destructive volume-removal flags when
restarting the laboratory.

## Verify Services

Inspect Kafka:

```bash
docker compose -p financial-streaming \
  -f compose.yaml ps
```

Inspect Flink:

```bash
docker compose -p financial-streaming-platform \
  -f compose.flink.yaml ps
```

Inspect monitoring:

```bash
docker compose -p financial-streaming-monitoring \
  -f compose.monitoring.yaml ps
```

The Flink REST API is available on the configured
endpoint, which was `127.0.0.1:8081` in the
validated local environment.

Service availability alone does not demonstrate
successful data processing.

## Execute the End-to-End Pipeline

The main entry point is:

`scripts/run_e2e_final.py`

Example:

```bash
E2E_MIN_EVENTS=3000 \
E2E_MIN_DURATION_SECONDS=180 \
E2E_MAX_EVENTS=250000 \
E2E_MAX_DURATION_SECONDS=360 \
.venv/bin/python -u scripts/run_e2e_final.py
```

The executor generates a unique Run ID and uses
isolated Kafka and Paimon resources.

The minimum conditions define the required
collection volume and duration.

The maximum values provide safety boundaries.

These configuration values are specific to the
experiment and are not business rules.

## Execution Artifacts

Each run creates a directory such as:

```text
runtime/flink/paimon/live_runs/<run_id>/
```

The directory contains execution metadata and
persisted data artifacts.

The status journal is:

```text
runtime/flink/paimon/live_runs/<run_id>/status.json
```

Machine Learning outputs are saved under:

```text
runtime/flink/paimon/live_runs/<run_id>/ml_final/
```

The runtime directory is excluded from Git.

## Checkpoints and Savepoints

The collector stores its source progress in a
local checkpoint file.

Apache Flink uses checkpoints for state recovery
and savepoints for controlled job lifecycle
operations.

The successful E2E run completed three
stop-with-savepoint operations.

A savepoint is not an independent backup of all
source data, and its presence does not guarantee
successful restoration in every scenario.

## Recovery and Safety

Before retrying a failed E2E execution:

1. Inspect the run-specific status journal.
2. Identify the Flink jobs created by that run.
3. Check whether jobs are still running.
4. Preserve existing savepoints and warehouses.
5. Resolve the underlying failure before
   starting another isolated execution.

Do not blindly resubmit streaming jobs against
existing destinations.

A failed client command does not necessarily mean
that Flink rejected the job submission.

Do not delete Kafka volumes or Flink runtime state
as a routine troubleshooting step.

## Testing

Run the Python regression suite:

```bash
.venv/bin/python -m pytest -q
```

Validate Compose configuration without starting
or stopping containers:

```bash
docker compose -p financial-streaming \
  -f compose.yaml config --quiet

docker compose -p financial-streaming-platform \
  -f compose.flink.yaml config --quiet

docker compose -p financial-streaming-monitoring \
  -f compose.monitoring.yaml config --quiet
```

The validated regression suite reported
42 passed tests and 18 passed subtests.

## Security

- Do not commit credentials, tokens or secrets.
- Do not print sensitive values to terminal logs.
- Avoid logging source payloads or user identifiers.
- Use least-privilege access where feasible.
- Keep generated runtime data out of version control.
- Do not expose administrative service ports
  publicly without appropriate protections.

## Validated Execution

The completed E2E run `671370fce3cc`
successfully processed 8,345 real events.

Bronze and Silver reconciled completely.

Gold produced 17 finalized windows with
zero observed count mismatches.

Machine Learning training and inference completed.

All three Flink jobs were stopped with savepoints.

See [End-to-End Execution](../e2e/README.md)
for the complete results.

## Known Limitations

- Single-node development environment.
- No production high availability.
- Local runtime and source checkpoint storage.
- Bounded collection rather than a managed
  continuously running collector service.
- External source availability and replay limits.
- No universal exactly-once guarantee.
- Small ML evaluation sample.
- Clean-environment installation should be
  validated independently before claiming
  full environment portability.

## Related Documentation

- [Architecture](../architecture/README.md)
- [Ingestion](../ingestion/README.md)
- [Processing](../processing/README.md)
- [Observability](../observability/README.md)
- [Machine Learning](../machine-learning/README.md)
- [End-to-End](../e2e/README.md)
