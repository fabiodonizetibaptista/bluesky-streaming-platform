# Stream Processing — Apache Flink and Apache Paimon

## Purpose

The processing layer transforms Kafka events into
validated, persistent and analytics-ready datasets.

The project combines:

- Apache Flink for stateful stream processing.
- Apache Paimon for persistent analytical tables.
- A medallion architecture with Bronze, Silver
  and Gold data layers.
- Checkpoints and savepoints for managing Flink state.
- Event-time windows for streaming aggregation.

## Processing Architecture

```text
Apache Kafka
     |
     v
Apache Flink
     |
     v
Paimon Bronze
     |
     v
Apache Flink
     |
     +----------------> Paimon Quarantine
     |
     v
Paimon Silver
     |
     v
Apache Flink
     |
     | Event-time processing
     | Watermark progression
     | Tumbling windows (10 seconds)
     v
Paimon Gold
     |
     v
Analytical Features / Machine Learning
```

## Bronze — Source Data Persistence

**Implementation:** `sql/07_kafka_to_paimon_bronze.sql`

Bronze preserves the Kafka record representation
and transport metadata.

| Field | Purpose |
|---|---|
| `payload` | Minimized JSON event received from Kafka |
| `kafka_partition` | Kafka partition |
| `kafka_offset` | Offset within the partition |
| `kafka_timestamp` | Kafka record timestamp |

Bronze does not represent the complete original
Bluesky source event. Data minimization occurs
in the Python collector before publication.

The Bronze layer does not perform business-level
deduplication or event classification.

## Silver — Contract Validation and Data Quality

**Implementation:** `sql/08_paimon_bronze_to_silver.sql`

Silver reads Bronze and classifies records according
to the supported event contract.

Its responsibilities include:

- JSON contract validation.
- Type normalization.
- Event timestamp interpretation.
- Classification of supported operations.
- Routing rejected records to quarantine.
- Persistence of valid structured events.

### Valid and Quarantine Outputs

Valid events are written to the Silver valid sink.

Rejected events are written to a separate
quarantine sink for analysis.

The quarantine mechanism supports contract
quality management; it is not a claim that all
possible data quality issues are detected.

During the validated E2E run:

- Valid Silver records: 8,345.
- Quarantined records: 0.
- Bronze/Silver reconciliation: passed.

The result describes the observed collection,
not a guarantee that future events will always
pass validation.

## Gold — Streaming Analytical Aggregation

**Implementation:** `sql/16_paimon_silver_to_gold_live.sql`

Gold consumes structured Silver events and
aggregates them into ten-second event-time
tumbling windows.

### Processing Configuration

| Setting | Implemented behavior |
|---|---|
| Flink execution mode | Streaming |
| Event-time field | Timestamp derived from Silver |
| Window type | Tumbling |
| Window duration | 10 seconds |
| Watermark delay | 5 seconds |
| Aggregation strategy | One phase |
| Paimon source scan mode | `latest` |
| Gold output | Paimon table |

The implementation uses a one-phase aggregation
strategy because an earlier experimental
aggregation plan failed to produce the expected
final window outputs.

### Gold Output Contract

Each finalized window contains:

| Field | Meaning |
|---|---|
| `window_start` | Inclusive window start |
| `window_end` | Exclusive window end |
| `total_events` | Number of events in the window |
| `create_count` | Create operation count |
| `update_count` | Update operation count |
| `delete_count` | Delete operation count |

For each materialized window, the operation
counts should reconcile with the total event
count under the supported operation contract.

### Event Time vs. Processing Time

Event time describes when the source event occurred.

Processing time describes when Flink processes
the record.

These timestamps may differ due to transport
delays, buffering and runtime conditions.

By grouping events using event time, Gold
represents source activity intervals rather
than arbitrary processing intervals.

### Watermarks and Closed Windows

A watermark indicates progress in event time.

The implementation permits five seconds of
event-time lateness before a window is treated
as ready for final output.

A window is not necessarily finalized when
the Python collector reaches its event target.

Therefore, Silver may contain records belonging
to time intervals that do not yet have
materialized Gold results.

During the validated E2E execution:

- 17 Gold windows were finalized.
- 7,554 events were represented in Gold.
- 791 Silver events fell outside finalized windows.
- No finalized Gold window had a count mismatch.
- The Gold operator reported zero late drops.

The 791 events were accounted for; they must
not be described as lost data.

## Stateful Processing

Apache Flink maintains state for operations
that depend on previously processed records,
including window aggregations.

Checkpointing periodically captures
consistent execution state.

Savepoints allow controlled stopping while
preserving state for a potential compatible
restoration.

These mechanisms are distinct:

| Mechanism | Primary function |
|---|---|
| Collector cursor checkpoint | Source resumption |
| Flink checkpoint | Runtime state recovery |
| Flink savepoint | Controlled lifecycle and state preservation |
| Paimon table | Persistent data storage |

Successful checkpoint and savepoint creation
does not establish that every recovery scenario
has been tested.

## Execution Isolation

The E2E runner generates a unique Run ID.

That identifier determines isolated Kafka
topics and Paimon warehouse paths for the run.

This avoids mixing a new execution with
historical experimental records.

The executor also records job identifiers,
execution stages and savepoint locations
in its local status journal.

## Reconciliation

The E2E validation compares:

1. Bronze record count with confirmed ingestion.
2. Valid plus quarantined Silver records
   with the Bronze total.
3. Materialized Gold windows with source Silver
   records from their corresponding intervals.
4. Silver events outside finalized Gold windows
   as a separate quantity.

These checks demonstrate observed consistency
for the tested execution.

They do not constitute a universal exactly-once
guarantee.

## Validated Results

**Run ID:** `671370fce3cc`

| Metric | Result |
|---|---:|
| Confirmed real events | 8,345 |
| Bronze records | 8,345 |
| Silver valid records | 8,345 |
| Silver quarantine records | 0 |
| Finalized Gold windows | 17 |
| Gold event total | 7,554 |
| Silver outside Gold windows | 791 |
| Mismatched Gold windows | 0 |
| Gold late records dropped | 0 |
| Preserved savepoints | 3 |
| E2E execution | PASSED |

## Limitations

- No claim of global source-event uniqueness.
- No universal exactly-once delivery guarantee.
- The lab is not a highly available deployment.
- Gold finalization depends on watermark
  progression and event-time distribution.
- Savepoint compatibility and source replay
  requirements must be considered for recovery.
- The E2E runner is intended for a controlled
  portfolio demonstration, not orchestration
  of a production environment.

## Related Documentation

- [Architecture](../architecture/README.md)
- [Ingestion](../ingestion/README.md)
- [End-to-End Execution](../e2e/README.md)
- [Original Paimon Runbook](../paimon-runbook.md)
- [Original Silver Streaming Guide](../silver-streaming.md)
