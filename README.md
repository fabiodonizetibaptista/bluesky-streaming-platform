# Real-Time Social Streaming Platform

A data engineering portfolio project that ingests real public
activity events from Bluesky Jetstream v2 into Apache Kafka.

The project focuses on reliable streaming ingestion, event
contracts, recovery, data minimization, and automated testing.

## Project Objective

Build an event-driven data platform capable of collecting
public social activity metadata and preparing it for
near-real-time analytics.

The current milestone implements Bluesky-to-Kafka ingestion
and an experimentally validated Kafka-to-Apache-Flink Bronze pipeline.
Silver transformations, Gold analytics, and analytical serving remain planned.

## Architecture

```text
Bluesky Jetstream v2
        |
        v
Python Collector
        |
        | Validation and data minimization
        | Kafka delivery confirmation
        | Persistent source checkpoint
        v
Apache Kafka (KRaft)
        |
        v
Apache Flink 2.2.1 [BRONZE VALIDATED]
        |
        +--> Bronze: Kafka payload + transport metadata [VALIDATED]
        |       |
        |       v
        |   Silver: quality and event-time processing [PLANNED]
        |       |
        |       v
        |   Gold: analytical outputs [PLANNED]
        |
        +--> Quarantine / DLQ [PLANNED]
```

See [Architecture Design](docs/architecture.md) for technical
decisions, event contracts, recovery semantics, and limitations.

## Technology Stack

| Technology | Purpose | Status |
|---|---|---|
| Python | Streaming collector and automated tests | Implemented |
| Bluesky Jetstream v2 | Public event source | Integrated |
| Apache Kafka | Event broker | Implemented |
| Docker Compose | Local Kafka infrastructure | Implemented |
| GitHub Actions | Automated unit testing | Configured |
| Apache Flink 2.2.1 | Stateful stream processing and Bronze ingestion | Bronze validated |
| Flink Kafka SQL Connector | Kafka source integration | Validated locally |

## Implemented Capabilities

- Connection to Bluesky Jetstream v2 over WebSocket.
- Subscription to `app.bsky.feed.post` events.
- Validation of the minimum event envelope.
- Publication of minimized metadata to Kafka.
- Kafka acknowledgment before checkpoint advancement.
- Local checkpoint persistence and cursor-based restart.
- Bounded reconnect attempts with exponential backoff.
- Unit tests for success and failure scenarios.
- Automated Python CI configuration.

## Event Contract

Messages published by the collector include:

- `source`
- `source_seq`
- `collection`
- `operation`
- `event_time`
- `witnessed_at`

Post text, DIDs, and record identifiers are not stored
in the Kafka event projection.

The topic `bluesky.posts.raw` currently contains minimized
metadata rather than complete source records.

Earlier development and benchmark messages also exist
in the topic. Historical contents are not homogeneous.

## Run Locally

### Requirements

- Python 3.11 or newer
- Docker with Docker Compose
- Network access to Bluesky Jetstream

### Install

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

### Start Kafka

```bash
docker compose up -d
```

Provision the Kafka topics using the repository script:

```bash
bash scripts/create_topics.sh
```

### Set Up Apache Flink

Install the pinned Kafka SQL connector before starting Flink:

```bash
./scripts/install_flink_connector.sh
```

The installer validates the artifact's SHA-256 checksum. The JAR
is intentionally excluded from Git and mounted read-only into
both Flink containers.

Start Kafka and Flink using their separate Compose configurations:

```bash
docker compose up -d
docker compose -f compose.flink.yaml up -d
```

This project uses host networking for Flink as an environment-specific
workaround validated in GitHub Codespaces. The local filesystem stores
Flink checkpoints, savepoints, and Bronze outputs under `runtime/flink/`,
which is excluded from Git.

### Flink SQL Experiments

The repository contains four SQL experiments:

| File | Purpose |
|---|---|
| `sql/01_kafka_connection_test.sql` | Bounded Kafka connector connectivity test |
| `sql/02_contract_validation.sql` | Diagnostic classification of historical event payloads |
| `sql/03_kafka_to_bronze.sql` | Bounded Kafka-to-Bronze persistence test |
| `sql/04_bronze_recovery_test.sql` | Continuous Bronze pipeline for fault-recovery testing |

These are reproducible experiment definitions, not yet a production
deployment workflow. Existing output directories and Kafka startup
offset settings must be reviewed before rerunning the SQL files.

### Bronze Validation Results

In the local experiment, the Bronze filesystem sink preserved
143 Kafka records with their payload and transport metadata.
The payload bytes matched the Kafka messages in the tested dataset.

A controlled TaskManager termination produced subsequent task
execution attempts, followed by successful checkpoints.
The finalized experimental Bronze output contained 143 unique
partition-offset pairs, with no duplicates, missing offsets
within the observed range, or invalid JSON records.

A stop-with-savepoint operation completed successfully. Actual
restoration from that savepoint has not yet been tested.

These results demonstrate the observed experiment, not a general
end-to-end exactly-once guarantee.

### Run the Collector

```bash
python -m bluesky_streaming.ingest
```

The current collector processes a bounded number of
confirmed events and then exits.

Checkpoint state is stored locally under `checkpoints/`,
which is excluded from Git.

### Run Tests

```bash
python -m unittest discover -s tests -v
```

The unit tests use mocks and do not require an active
Bluesky WebSocket connection or Kafka broker.

## Delivery Guarantees

The ingestion design follows an at-least-once strategy
for events successfully received and processed.

Inclusive cursor replay and crashes between Kafka delivery
and checkpoint persistence may produce duplicates.

Downstream contract validation and deduplication are planned for Flink Silver.

End-to-end exactly-once processing is not claimed.

## Current Limitations

- Single-node Kafka deployment for local development.
- Bounded collector execution rather than a managed service.
- Local filesystem checkpoint.
- Synchronous publication per event.
- Flink Bronze has been validated experimentally; Silver, Gold, and analytical storage are not implemented.
- No implemented quarantine/DLQ.
- Historical Kafka topic contains experimental messages.
- Long outages may exceed the source's replay availability.
- CI execution on GitHub remains to be verified.

## Roadmap

1. Complete Bluesky-to-Kafka ingestion foundation.
2. Consolidate event contracts, documentation, and CI.
3. Consolidate and document the validated Apache Flink Bronze pipeline.
4. Implement Silver contract validation, event-time processing, and deduplication.
5. Implement quarantine, data quality, and operational observability.
6. Build Gold analytical outputs and portfolio documentation.

## Security and Privacy

The collector minimizes the data retained from public
Jetstream events.

Credentials must not be committed to the repository.
Logs should not expose post content, personal identifiers,
or secrets.

This project is intended for learning and portfolio
demonstration, not production deployment.
