# Observability — Prometheus and Grafana

## Purpose

The observability layer supports visibility into
the operational behavior of the streaming platform.

It complements the data pipeline by helping
investigate processing activity, resource usage,
service availability, and execution health.

The project uses:

- Prometheus for collecting time-series metrics.
- Grafana for visualizing operational information.
- Docker Compose for deploying the monitoring stack.

This is an operational monitoring demonstration,
not a production observability platform.

## Architecture

```text
Streaming Infrastructure
         |
         | Exposed Metrics
         v
     Prometheus
         |
         | Metrics Queries
         v
       Grafana
         |
         v
 Operational Dashboards
```

The monitoring configuration is defined in:

`compose.monitoring.yaml`

Dashboard preparation utilities are available in:

`scripts/build_observability_dashboard.py`

## Prometheus

Prometheus stores numerical monitoring data
as time series.

The monitoring architecture is intended to
provide information such as:

- Availability of monitored services.
- Processing throughput.
- Resource utilization.
- Operational behavior over time.

The actual metrics exposed depend on the
configured scrape targets and exporters.

Not all desirable streaming metrics are
necessarily available in the current lab.

## Grafana

Grafana provides visualization of the metrics
collected by Prometheus.

Dashboards can help identify changes in
resource utilization and processing behavior.

Operational visualizations are complementary
to data-quality and business analytics.

A healthy infrastructure dashboard alone
does not prove that all data has been
processed correctly.

## Observability vs. Data Reconciliation

The platform uses two distinct approaches
to understanding execution behavior.

### Operational Monitoring

Prometheus and Grafana support observation of
infrastructure and processing characteristics.

### Data Reconciliation

The E2E runner compares the results persisted
across Bronze, Silver and Gold.

This verifies the consistency of the tested
data flow under the observed conditions.

Both capabilities are valuable, but they
answer different questions.

## Monitoring Deployment

The monitoring services are described in:

`compose.monitoring.yaml`

The existing Docker Compose project name is
retained to preserve the current lab setup.

Before starting or modifying monitoring
services, review the Compose configuration
and its volume mappings.

Do not remove persistent volumes as part
of routine environment preparation.

## Observed Project Evidence

The completed E2E run demonstrated:

- Successful Flink job submission.
- Completed streaming checkpoints.
- Controlled shutdown with three savepoints.
- Bronze/Silver data reconciliation.
- Gold window reconciliation.
- A final successful E2E execution status.

These are execution results recorded by the
pipeline workflow, not equivalent to a formal
Prometheus monitoring benchmark.

See [End-to-End Execution](../e2e/README.md)
for the detailed observed results.

## Limitations

- Single-node lab environment.
- No production alerting SLA.
- No demonstrated high-availability monitoring.
- Monitoring coverage depends on enabled
  metrics endpoints and scrape targets.
- Full distributed tracing is not established.
- Business-level data-quality guarantees
  cannot be inferred from infrastructure
  metrics alone.

## Related Documentation

- [Architecture](../architecture/README.md)
- [Processing](../processing/README.md)
- [End-to-End Execution](../e2e/README.md)
