# Real-Time Social Streaming Platform — Architecture Design

## 1. Purpose and Scope

Build a reproducible, resource-conscious streaming data engineering
platform using public activity metadata from Bluesky Jetstream v2.

The platform demonstrates real event ingestion, Kafka messaging,
Apache Flink stream processing, state recovery, and the progressive
development of Bronze, Silver, and Gold data layers.

This is an engineering portfolio project, not a production deployment.

### Implementation Status

| Capability | Status |
|---|---|
| Bluesky Jetstream v2 collection | Implemented and tested |
| Kafka ingestion and topic provisioning | Implemented and tested |
| Flink Kafka SQL connectivity | Validated locally |
| Kafka-to-Bronze ingestion | Experimentally validated |
| TaskManager failure recovery | Experimentally validated |
| Stop-with-savepoint | Completed |
| Restore from savepoint | Not yet tested |
| Silver processing | Planned |
| Gold analytical outputs | Planned |
| Quarantine and operational observability | Planned |

## 2. Current Architecture (AS-IS)

```text
Bluesky Jetstream v2
        |
        | WebSocket / XRPC
        v
Python Collector (bluesky_streaming)
        |
        | Validate minimum source envelope
        | Minimize source data for privacy
        | Await Kafka delivery acknowledgement
        | Persist confirmed source cursor
        v
Apache Kafka 4.2.2 (single-node KRaft)
        |
        | Topic: bluesky.posts.raw
        | Minimized JSON messages
        v
Apache Flink 2.2.1
        |
        | Kafka SQL source
        | Payload and Kafka transport metadata
        v
Bronze filesystem output (JSON Lines)
        |
        +--> Silver [PLANNED]
                 |
                 +--> Gold [PLANNED]
```

The collector and Flink have independent recovery mechanisms.

The collector persists a source cursor after Kafka acknowledges an
event. Flink uses checkpoints to recover its streaming execution
state. Neither mechanism replaces the other.

## 3. Source Collection and Privacy

The Python collector subscribes to the Bluesky Jetstream v2
`app.bsky.feed.post` collection and processes supported commit events.

Only the following event projection is published to Kafka:

| Field | Meaning |
|---|---|
| `source` | Constant `bluesky_jetstream_v2` |
| `source_seq` | Jetstream v2 source cursor |
| `collection` | Source collection |
| `operation` | Source operation |
| `event_time` | Event timestamp |
| `witnessed_at` | Time at which the event was witnessed |

The collector deliberately excludes post text, DIDs, original record
identifiers, and the full original Bluesky source message.

Data minimization takes place before publication to Kafka.
Consequently, Bronze preserves the Kafka message, not the complete
original Bluesky message.

Minimization does not establish that retained metadata is anonymous
or exempt from privacy requirements.

## 4. Kafka Ingestion and Delivery

Kafka runs as a single-node KRaft broker through `compose.yaml`.

The event topic is `bluesky.posts.raw`. Despite the `raw` suffix,
the current producer publishes a minimized JSON projection.

The topic contains both real Bluesky event projections and earlier
experimental/test messages. Its historical data must not be assumed
to satisfy one homogeneous business event contract.

### Producer Guarantees

The publisher uses Kafka acknowledgement confirmation and producer
idempotence. Once a send is acknowledged, the collector advances its
local source checkpoint.

If publication succeeds but source checkpoint persistence fails,
the next collection run can replay an event.

The architecture therefore follows an at-least-once ingestion
approach for events received and successfully handled by the
collector. End-to-end exactly-once delivery is not claimed.

The Jetstream v2 sequence is a source cursor, not a globally stable
event ID across independent Jetstream servers.

## 5. Processing Architecture and Data Layers

Apache Flink 2.2.1 is the selected streaming processing engine.
The prior Spark Structured Streaming proposal was superseded.

### Bronze — Implemented Experimentally

Bronze persists the original message payload received from Kafka,
together with Kafka transport metadata:

| Field | Description |
|---|---|
| `payload` | Message payload as received from Kafka |
| `kafka_partition` | Source Kafka partition |
| `kafka_offset` | Source Kafka offset |
| `kafka_timestamp` | Kafka record timestamp |

Bronze does not attempt to classify records as valid Bluesky events,
clean business fields, or deduplicate source events.

A Kafka partition-offset pair provides a transport-level position;
it is not a universal identifier for a real-world source event.

The bounded Bronze experiment is defined in
`sql/03_kafka_to_bronze.sql`.

The continuous recovery experiment is defined in
`sql/04_bronze_recovery_test.sql`.

Both write to local filesystem destinations under `runtime/flink/`.

### Silver — Planned

Silver will be responsible for applying the data contract and
processing semantics to Bronze records, including:

- JSON structure and mandatory field validation.
- Type normalization and timestamp interpretation.
- Differentiation of valid source records and experimental records.
- Explicit handling of invalid or unsupported messages.
- Event-time processing and watermark strategy.
- Deduplication according to a documented identity and replay policy.

The deduplication key and retention policy must be validated before
implementation. The Jetstream cursor alone is not sufficient to
guarantee global source-event identity.

### Gold — Planned

Gold will expose analytical products derived from validated Silver
data, such as activity volumes, time-window aggregates, and
operationally meaningful trends.

Gold metrics, output storage, and serving interfaces have not yet
been implemented or selected.

## 6. Local Runtime and Infrastructure

| Component | Version / Configuration |
|---|---|
| Kafka | `apache/kafka:4.2.2` |
| Flink | `flink:2.2.1-scala_2.12-java17` |
| Kafka SQL connector | `flink-sql-connector-kafka-5.0.0-2.2.jar` |
| Python collector | Python >= 3.11 |
| Orchestration | Docker Compose for local development |

The Kafka SQL connector is installed with
`scripts/install_flink_connector.sh`, using a pinned artifact version
and SHA-256 verification. Its binary is excluded from Git.

Flink operates with one JobManager, one TaskManager, and one configured
task slot. Memory limits are intentionally small to accommodate the
available GitHub Codespaces resources.

Flink containers currently use `network_mode: host` as a workaround
for observed Docker bridge communication failures in Codespaces.
This is an environment-specific decision, not a recommended default
for production networks.

### Persistent Storage

- Kafka broker state: Docker volume `financial-streaming_kafka_data`.
- Collector source cursor: ignored local `checkpoints/` directory.
- Flink state and Bronze outputs: ignored local `runtime/flink/`
  directory.

The Kafka Compose project retains the legacy name
`financial-streaming` to preserve its existing Docker volume mapping.
Renaming that Compose project or its volume requires a separately
validated data migration.

The Git repository and workspace also retain their legacy names
temporarily. The public-facing project identity is
**Real-Time Social Streaming Platform**.

## 7. Checkpoints, Savepoints, and Recovery

### Collector Checkpoint

The collector persists the last Kafka-confirmed Jetstream cursor in
a local JSON file using temporary-file writing, filesystem sync, and
atomic replacement.

This supports source replay after collector interruption, subject
to source availability and cursor semantics.

### Flink Checkpoints

Flink checkpoints are configured with a 30-second interval and
stored under `runtime/flink/checkpoints/`.

Checkpoints support recovery of running Flink jobs after supported
execution failures. They do not make this single-node deployment
highly available.

### TaskManager Failure Experiment

A continuous Kafka-to-Bronze Flink job was submitted and monitored.

During the experiment:

1. The job was confirmed `RUNNING`, with nine completed checkpoints.
2. The TaskManager was terminated using `SIGKILL`.
3. The TaskManager container was restarted.
4. The same Flink job ID returned to an observed `RUNNING` state.
5. The task reported execution attempt `2`, and the job exposed
   two failure-history entries.
6. Completed checkpoints subsequently advanced from 9 to 16.
7. The finalized experimental Bronze output contained 143 records,
   143 distinct partition-offset pairs, zero duplicates, zero
   invalid JSON records, and no missing offsets from 0 through 142.

These observations support recovery of execution and consistency of
the finalized records in this particular test.

The experiment did not establish a universal exactly-once guarantee
or test source events arriving during the TaskManager interruption.

### Stop-with-Savepoint Experiment

The recovery-test job was stopped through the Flink CLI using a
canonical stop-with-savepoint operation without `--drain`.

The successful command returned:

`runtime/flink/savepoints/savepoint-c5f020-ffc7de51dbfe`

The savepoint's `_metadata` file was verified after restarting
the Codespaces workspace.

Restoring a job from that savepoint has not yet been tested.
The savepoint itself does not contain a durable copy of all Kafka
source events; restoration also depends on source offset availability.

## 8. SQL Experiment Inventory

| File | Purpose |
|---|---|
| `sql/01_kafka_connection_test.sql` | Bounded Kafka connector read |
| `sql/02_contract_validation.sql` | Diagnostic JSON and contract classification |
| `sql/03_kafka_to_bronze.sql` | Bounded Kafka-to-Bronze filesystem test |
| `sql/04_bronze_recovery_test.sql` | Continuous Bronze and failure-recovery test |

The contract-validation SQL is diagnostic work, not an implemented
Silver data pipeline.

Re-executing these experiments requires reviewing Kafka startup
offsets, output paths, and existing persisted state to avoid
accidental replay or duplicated filesystem outputs.

## 9. Constraints and Open Risks

- A single Kafka broker is not a highly available architecture.
- Local Flink state storage does not provide cluster-level HA.
- Recovery after JobManager loss has not been demonstrated.
- The collector uses bounded execution and synchronous publication.
- The Kafka topic contains heterogeneous historical test data.
- Source retention may limit replay after extended interruptions.
- Savepoint restoration has not been demonstrated.
- Silver quality rules and identity semantics are not implemented.
- Quarantine/DLQ, Gold metrics, and serving are not implemented.
- A clean-environment deployment and GitHub Actions CI run remain
  to be validated.
- The current infrastructure is for local development and learning.

## 10. Next Engineering Milestones

1. Consolidate connector installation, deployment documentation,
   automated tests, and the first pull request.
2. Validate reproducibility and document restore procedures.
3. Design Silver data contracts, event-time semantics, and
   deduplication rules.
4. Implement and validate Silver processing and quarantine.
5. Design and implement Gold analytical outputs.
6. Add operational observability and portfolio evidence.

Each subsequent architectural decision should evaluate practical
learning value, relevant market capabilities, free-tier limitations,
runtime resource costs, and alternatives before implementation.

## 11. Engineering Principles

- Evidence before claims about reliability.
- Separation between AS-IS implementation and TO-BE design.
- Data minimization and secure defaults.
- Explicit contracts and documented state semantics.
- Reproducibility, automated testing, and code review.
- Preservation of existing persistent data during refactoring.
- Resource-aware operation in GitHub Codespaces.
