# Bluesky Streaming Platform — Architecture

## Overview

Bluesky Streaming Platform is a data engineering
portfolio project built around real public activity
events from Bluesky Jetstream.

It demonstrates event-driven ingestion, stateful
stream processing, analytical data modeling,
operational observability, and machine learning.

The architecture is designed for a reproducible
single-node lab, not a production deployment.

## End-to-End Architecture

```text
Bluesky Jetstream v2
         |
         v
Python Collector
         |
         | Event validation
         | Data minimization
         | Cursor checkpoint
         v
Apache Kafka (KRaft)
         |
         v
Apache Flink
         |
         v
Apache Paimon — Bronze
         |
         v
Apache Flink
         |
         +----------> Paimon Quarantine
         |
         v
Apache Paimon — Silver
         |
         v
Apache Flink
         |
         | Event-time windows
         | Watermark progression
         v
Apache Paimon — Gold
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

Prometheus and Grafana provide operational
monitoring for supported infrastructure components.

## Main Components

| Component | Responsibility |
|---|---|
| Bluesky Jetstream | Public event source |
| Python collector | Validate, minimize and publish events |
| Apache Kafka | Event transport and buffering |
| Apache Flink | Stateful streaming transformations |
| Apache Paimon | Persistent Bronze, Silver and Gold layers |
| Prometheus | Infrastructure metrics collection |
| Grafana | Operational dashboards |
| Isolation Forest | Experimental anomaly detection |

## Medallion Architecture

### Bronze

The Bronze layer ingests Kafka records and
preserves their payload together with transport
metadata, including partition, offset and timestamp.

The payload is the minimized event projection
published by the collector, not the complete
original Bluesky record.

### Silver

The Silver layer applies event contract validation,
type normalization and timestamp interpretation.

Supported records are written to the valid-data
table. Rejected records follow a separate
quarantine path.

The validated E2E execution processed 8,345
records into Silver, with zero quarantined records.

### Gold

The Gold layer uses event-time tumbling windows
of ten seconds.

Apache Flink processes the Silver stream using
a watermark strategy that allows five seconds
of event-time lateness.

The selected aggregation uses one-phase processing
and writes finalized window results to Paimon.

The Gold output includes event volume and counts
of create, update and delete operations.

Only finalized windows are included in the
corresponding batch analytical output.

## Reliability

### Source checkpoint

The collector advances its local checkpoint only
after receiving Kafka publication confirmation.

Inclusive source cursor replay can lead to
reprocessed events.

### Flink state

Flink uses checkpoints to support stateful
execution and savepoints for controlled shutdown.

The E2E executor generates isolated run-specific
topics and storage paths.

These mechanisms improve recoverability, but
do not establish universal exactly-once delivery.

### Data reconciliation

The E2E workflow reconciles Kafka-derived
Bronze and Silver records, compares finalized
Gold windows with the corresponding Silver data,
and accounts separately for Silver events outside
finalized windows.

## Machine Learning

Gold aggregations are converted into analytical
features and consumed by an Isolation Forest model.

The model uses a chronological training/evaluation
split.

This demonstrates feature engineering, training,
model persistence and inference.

The available dataset does not establish
production-level anomaly detection performance.

## Validated Execution

Run ID: `671370fce3cc`

| Metric | Result |
|---|---:|
| Real events collected | 8,345 |
| Bronze records | 8,345 |
| Valid Silver records | 8,345 |
| Finalized Gold windows | 17 |
| Events in finalized Gold windows | 7,554 |
| Silver events outside finalized windows | 791 |
| Mismatched Gold windows | 0 |
| Late records dropped | 0 |
| Flink savepoints | 3 |
| ML training/evaluation windows | 11 / 6 |
| End-to-end result | PASSED |

Refer to [End-to-End Execution](../e2e/README.md)
for execution instructions and evidence interpretation.

## Design Boundaries

- Single-node lab infrastructure.
- Local filesystems and persistent Docker volumes.
- Bounded event collection.
- At-least-once ingestion characteristics.
- No general end-to-end exactly-once guarantee.
- No production high-availability claim.
- Limited ML evaluation sample.

## Further Documentation

- [End-to-End Execution](../e2e/README.md)
- [Legacy Architecture Design](../architecture.md)
- [Paimon Runbook](../paimon-runbook.md)
- [Silver Streaming](../silver-streaming.md)

The legacy documents are retained as technical
development history until their relevant details
have been consolidated.
