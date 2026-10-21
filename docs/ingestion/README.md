# Ingestion — Bluesky Jetstream to Kafka

## Purpose

The ingestion layer collects public activity events
from Bluesky Jetstream v2 and publishes a minimized
event projection to Apache Kafka.

The primary implementation is:

`src/bluesky_streaming/`

The collector is designed to demonstrate streaming
ingestion, explicit delivery confirmation, failure
handling and controlled source checkpointing.

## Data Flow

```text
Bluesky Jetstream v2
        |
        | WebSocket
        v
Python Collector
        |
        | Validate source envelope
        | Select supported events
        | Minimize retained data
        v
Kafka Producer
        |
        | Wait for delivery confirmation
        v
Apache Kafka
        |
        v
Persistent Source Cursor
```

## Source

The collector subscribes to the Jetstream collection:

`app.bsky.feed.post`

It processes supported commit messages and uses
the Jetstream cursor for source resumption.

The cursor is a source progress marker and must
not be treated as a globally unique event ID
across independent Jetstream servers.

## Published Event Contract

The Kafka event projection contains:

| Field | Description |
|---|---|
| `source` | Source system identifier |
| `source_seq` | Jetstream source cursor |
| `collection` | Source collection |
| `operation` | Source operation |
| `event_time` | Source event timestamp |
| `witnessed_at` | Timestamp when the event was witnessed |

The Kafka topic does not store the complete
original Bluesky source record.

Post text, DIDs and original record identifiers
are excluded from the event projection.

This reduces retained personal information,
although it does not guarantee anonymity.

## Kafka Delivery

The producer waits for a successful Kafka
delivery acknowledgement before advancing
the persistent source cursor.

The ingestion flow follows at-least-once
delivery characteristics.

An event may be replayed if Kafka acknowledges
the publication but the subsequent local
checkpoint update fails.

End-to-end exactly-once delivery is not claimed.

## Checkpoints and Recovery

The collector persists the most recently
confirmed source cursor.

Checkpoint persistence uses a temporary file,
filesystem synchronization and atomic replacement
to reduce the risk of incomplete writes.

On restart, the collector can resume using
the stored cursor, subject to source availability.

Because cursor replay can include previously
processed messages, downstream consumers must
not assume every received event is globally unique.

## Reconnection

The collector handles transport failures
separately from processing failures.

Supported transport interruptions trigger
bounded reconnection attempts with exponential
backoff.

Processing errors are treated as fatal
instead of being silently retried as
connection failures.

The integration experiment also demonstrated
reconnection following a WebSocket disconnection.

## Configurable E2E Collection

The E2E runner uses configurable event and
duration boundaries.

| Setting | Description |
|---|---|
| `E2E_MIN_EVENTS` | Minimum confirmed event count |
| `E2E_MIN_DURATION_SECONDS` | Minimum collection duration |
| `E2E_MAX_EVENTS` | Maximum permitted event count |
| `E2E_MAX_DURATION_SECONDS` | Maximum collection duration |

These settings define the experimental execution
contract; they are not business thresholds.

The runner assigns each E2E execution a
unique Kafka topic and checkpoint location,
preventing accidental mixing of independent runs.

## Limitations

- Collection depends on the availability of
  the Bluesky Jetstream service.
- The source's replay availability is external
  to this application.
- Synchronous per-event publishing limits
  potential throughput.
- The local checkpoint is not a distributed
  high-availability service.
- Source cursor replay may generate duplicates.
- The single-broker Kafka lab is not designed
  for production high availability.

## Validation

The completed E2E execution
`671370fce3cc` confirmed:

| Metric | Result |
|---|---:|
| Real events collected | 8,345 |
| Bronze persisted records | 8,345 |
| Valid Silver records | 8,345 |
| Quarantined records | 0 |

The experiment demonstrates successful
end-to-end publication and persistence
under the observed test conditions.

For the complete execution results, see
[End-to-End Execution](../e2e/README.md).

## Related Documentation

- [Architecture](../architecture/README.md)
- [End-to-End Execution](../e2e/README.md)
- [Original Architecture Design](../architecture.md)
