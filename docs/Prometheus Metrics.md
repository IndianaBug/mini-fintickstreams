
This document describes the custom metrics exposed by `mini-fintickstreams`.

The goal is not to document every possible PromQL query, but to explain what each metric represents and which ones are most useful when operating or debugging the service.

## Quick Navigation

- [[#Most Important Metrics]]
- [[#Application and Runtime]]
- [[#Ingest and WebSocket]]
- [[#Database]]
- [[#Redis]]
- [[#Useful PromQL]]
- [[#Metric Types]]

---

## Most Important Metrics

If you only monitor a small dashboard, start with these:

| Metric | Why it matters |
|---|---|
| **`app_health`** | Overall application health. `1` = healthy. |
| **`runtime_health`** | Runtime protection state. `1` = GREEN, `0` = RED. |
| **`streams_active`** | Number of currently running market-data streams. |
| **`ingest_processed_total`** | Confirms market-data messages are actually being processed. |
| **`ingest_errors_total`** | Shows parsing, transport, or handler failures. |
| **`ingest_lag_seconds`** | Shows how far behind real time ingestion is. |
| **`ws_reconnect_attempts_total`** | Good indicator of unstable exchange WebSocket connections. |
| **`db_health_state`** | Database health: `0` green, `1` yellow, `2` red. |
| **`db_rows_written_total`** | Confirms data is actually reaching PostgreSQL/TimescaleDB. |
| **`db_write_latency_seconds`** | Measures database batch-write latency. |
| **`db_failed_batches_total`** | Detects failed database writes. |
| **`db_rows_dropped_total`** | Particularly important: data was discarded. |
| **`db_writer_queue_depth`** | Shows DB writer backpressure. |
| **`redis_enabled_state`** | Whether Redis is currently usable by the application. |

Together these answer the most important operational questions:

```text
Is the application healthy?
Are streams running?
Is market data arriving?
Are we falling behind?
Are WebSockets stable?
Is data reaching the database?
Is the database overloaded?
Are we losing data?
Is Redis available?
```

---

## Application and Runtime

### Health

| Metric | Meaning |
|---|---|
| **`app_health`** | Overall application health. `1` healthy, `0` unhealthy. |
| `app_ready` | Whether the application is ready to serve traffic. |
| **`runtime_health`** | Runtime guard state. `1` GREEN, `0` RED. |
| `app_info` | Static information such as application ID, environment, and config version. |

### Runtime RED Reasons

These explain **why** the runtime entered RED state:

| Metric | Meaning |
|---|---|
| `runtime_health_red_rss` | Process memory usage exceeded its configured limit. |
| `runtime_health_red_avail_mem` | System available memory became too low. |
| `runtime_health_red_fd` | File descriptor usage became too high. |
| `runtime_health_red_drift` | Tokio scheduling/tick delay exceeded the threshold. |
| `runtime_health_red_cpu` | Sustained CPU usage exceeded the configured threshold. |

Each is a `0/1` gauge.

For example:

```promql
runtime_health_red_cpu{job="mini-fintickstreams"}
```

### Runtime State Changes

| Metric | Meaning |
|---|---|
| `runtime_health_to_red_total` | Number of transitions into RED state. |
| `runtime_health_to_green_total` | Number of recoveries back to GREEN. |
| `streams_add_denied_runtime_red_total` | Stream starts rejected because runtime health was RED. |

Frequent RED/GREEN transitions usually indicate resource pressure or thresholds that need investigation.

---

## Stream Control

| Metric | Meaning |
|---|---|
| **`streams_active`** | Current number of active streams. |
| `streams_limit` | Configured maximum number of active streams. |
| `streams_add_total` | Stream start operations performed. |
| `streams_remove_total` | Stream removal operations performed. |
| `streams_update_total` | Stream update operations performed. |
| `streams_op_errors_total` | Errors from stream-control operations. |
| `streams_add_denied_db_total` | Stream starts rejected because of DB health/admission checks. |
| `streams_add_denied_redis_total` | Stream starts rejected because Redis could not accept a new symbol. |

A useful capacity query is:

```promql
streams_active{job="mini-fintickstreams"}
/
streams_limit{job="mini-fintickstreams"}
* 100
```

This gives the percentage of configured stream capacity currently in use.

---

## Ingest and WebSocket

These metrics describe the actual market-data ingestion pipeline.

### Throughput

| Metric | Meaning |
|---|---|
| **`ingest_in_total`** | Messages received from WebSocket or HTTP sources. |
| **`ingest_processed_total`** | Messages successfully processed and normalized. |
| `ingest_acked_total` | Messages acknowledged/committed where applicable. |

Usually the most useful form is a rate:

```promql
rate(ingest_processed_total{job="mini-fintickstreams"}[5m])
```

This shows approximately:

```text
processed messages / second
```

### Errors and Quality

| Metric | Meaning |
|---|---|
| **`ingest_errors_total`** | Transport, parsing, or handler errors. |
| `ingest_retried_total` | Operations that had to be retried. |
| `ingest_duplicates_total` | Duplicate deliveries detected. |

Useful:

```promql
increase(ingest_errors_total{job="mini-fintickstreams"}[5m])
```

A healthy ingestion pipeline should normally have a high processing rate and a very low error rate.

### Lag and Backpressure

| Metric | Meaning |
|---|---|
| **`ingest_lag_seconds`** | End-to-end delay between incoming data and processing. |
| `ingest_queue_depth` | Approximate number of pending ingest items. |

These are especially useful during bursts of market activity.

Increasing queue depth together with increasing lag usually means:

```text
incoming data
      ↓
arrives faster than processing
      ↓
queue grows
      ↓
latency increases
```

---

### WebSocket Stability

| Metric | Meaning |
|---|---|
| **`ws_reconnect_attempts_total`** | Number of WebSocket reconnect attempts. |
| `ws_reconnect_rate_limited_total` | Reconnects delayed by the reconnect limiter. |
| `ws_reconnect_wait_seconds` | Time spent waiting before reconnecting. |
| `ws_subscribe_attempts_total` | WebSocket subscription attempts. |
| `ws_subscribe_rate_limited_total` | Subscription attempts throttled by the limiter. |
| `ws_subscribe_wait_seconds` | Time spent waiting for permission to subscribe. |

A useful reconnect query:

```promql
increase(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
)
```

Normally this should remain close to zero.

A sudden increase can indicate:

```text
exchange instability
network problems
heartbeat failures
connection timeouts
rate-limit pressure
```

---

### Rate Limiting

| Metric | Meaning |
|---|---|
| `ingest_rate_limited_total` | Number of times ingest was blocked or delayed by rate limiting. |
| `ingest_rate_limit_wait_seconds` | Time spent waiting for rate-limiter permits. |

These become particularly useful when many streams are started simultaneously or exchange limits become restrictive.

---

## Database

Database metrics describe the PostgreSQL/TimescaleDB write path.

### Write Throughput

| Metric | Meaning |
|---|---|
| **`db_rows_written_total`** | Total rows successfully written. |
| `db_batches_written_total` | Total batches successfully written. |
| `db_rows_per_batch` | Distribution of batch sizes. |
| `db_rows_enqueued_total` | Rows intended/enqueued for writing. |
| `db_batches_enqueued_total` | Batches intended/enqueued for writing. |

One of the best database graphs is:

```promql
rate(db_rows_written_total{job="mini-fintickstreams"}[5m])
```

It shows:

```text
database rows written / second
```

If market data is flowing but this becomes zero, the DB write path deserves attention.

---

### Database Latency

| Metric | Meaning |
|---|---|
| **`db_write_latency_seconds`** | Time required to write a batch. |
| `db_flush_delay_seconds` | Time between enqueueing data and flushing it. |
| `db_queue_wait_seconds` | Time waiting for a writer permit. |
| `db_pool_wait_seconds` | Time waiting for a PostgreSQL connection. |
| `db_stalled_flush_seconds` | Time spent stalled in the flush path. |

These help distinguish different bottlenecks:

```text
high db_write_latency_seconds
    → PostgreSQL writes are slow

high db_pool_wait_seconds
    → connection pool is saturated

high db_queue_wait_seconds
    → writer concurrency is saturated

high db_flush_delay_seconds
    → data waits too long before being flushed
```

---

### Database Backpressure

| Metric | Meaning |
|---|---|
| **`db_writer_queue_depth`** | Approximate number of DB writes currently competing for writer capacity. |
| **`db_oldest_batch_age_seconds`** | Age of the oldest pending batch. |
| `db_pool_in_use` | DB connections currently being used. |
| `db_pool_idle` | Idle DB connections. |
| `db_pool_max` | Maximum configured connection pool size. |

`db_oldest_batch_age_seconds` is particularly useful because it can reveal a stuck writer even when latency histograms stop receiving new observations.

---

### Database Failures

| Metric | Meaning |
|---|---|
| **`db_failed_batches_total`** | Database batches that failed to write. |
| `db_retried_batches_total` | Failed batches that were retried. |
| **`db_rows_dropped_total`** | Rows discarded instead of being written. |
| `db_errors_total` | Database errors grouped by error `kind`. |

`db_rows_dropped_total` should normally remain:

```text
0
```

Any increase deserves investigation because it represents lost ingestion data.

---

### Database Health

```text
db_health_state
```

Values:

```text
0 = GREEN
1 = YELLOW
2 = RED
```

Query:

```promql
db_health_state{job="mini-fintickstreams"}
```

This combines several DB pressure signals into a simple operational state.

---

## Redis

Redis is optional acceleration/application infrastructure, so its metrics are mainly concerned with publish performance and whether Redis remains usable.

| Metric | Meaning |
|---|---|
| **`redis_enabled_state`** | `1` when Redis is currently usable, `0` when disabled. |
| `redis_stream_published_total` | Messages successfully published to Redis Streams. |
| **`redis_publish_latency_seconds`** | Redis publish latency. |
| **`redis_publish_failures_total`** | Failed Redis publishes. |
| `redis_publish_queue_depth` | Approximate application-side publish backlog. |
| `redis_disable_events_total` | Times Redis was disabled, labeled by reason. |

Useful:

```promql
redis_enabled_state{job="mini-fintickstreams"}
```

and:

```promql
increase(
  redis_publish_failures_total{job="mini-fintickstreams"}[5m]
)
```

`redis_disable_events_total` can help explain why Redis was removed from the active path:

```promql
sum by (reason) (
  redis_disable_events_total{job="mini-fintickstreams"}
)
```

Possible reasons can include health, latency, saturation, or manual disabling.

---

## Config Metrics

| Metric | Meaning |
|---|---|
| `config_reload_total` | Configuration reload attempts. |
| `config_reload_errors_total` | Failed configuration reloads. |

These are mostly useful when configuration is changed dynamically.

---

## Useful PromQL

### Is the Application Healthy?

```promql
app_health{job="mini-fintickstreams"}
```

### How Many Streams Are Running?

```promql
streams_active{job="mini-fintickstreams"}
```

### Messages Processed Per Second

```promql
rate(
  ingest_processed_total{job="mini-fintickstreams"}[5m]
)
```

### Ingest Errors During the Last 5 Minutes

```promql
increase(
  ingest_errors_total{job="mini-fintickstreams"}[5m]
)
```

### WebSocket Reconnects During the Last 5 Minutes

```promql
increase(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
)
```

### Database Rows Written Per Second

```promql
rate(
  db_rows_written_total{job="mini-fintickstreams"}[5m]
)
```

### Database Health

```promql
db_health_state{job="mini-fintickstreams"}
```

### Data Loss

```promql
increase(
  db_rows_dropped_total{job="mini-fintickstreams"}[5m]
)
```

For normal operation this should ideally return:

```text
0
```

---

## Metric Types

The project mainly uses three Prometheus metric types.

### Counter

Counters only increase until the process restarts.

Examples:

```text
ingest_processed_total
ws_reconnect_attempts_total
db_rows_written_total
db_failed_batches_total
```

Usually use:

```promql
rate(metric_total[5m])
```

or:

```promql
increase(metric_total[5m])
```

### Gauge

Gauges represent current state and can move up or down.

Examples:

```text
app_health
streams_active
db_writer_queue_depth
redis_enabled_state
```

Usually query them directly.

### Histogram

Histograms record distributions such as latency or batch size.

Examples:

```text
ingest_lag_seconds
db_write_latency_seconds
db_rows_per_batch
redis_publish_latency_seconds
```

Prometheus exposes histogram components such as:

```text
_metric_bucket
_metric_sum
_metric_count
```

These are generated automatically and normally do not need separate documentation.

For latency dashboards, histograms can be converted into percentiles such as p95:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(db_write_latency_seconds_bucket{job="mini-fintickstreams"}[5m])
  )
)
```

---

## Practical Monitoring Priorities

For a small deployment or demonstration, there is no need to graph every metric.

A useful dashboard can start with:

```text
Application
    app_health
    streams_active

Ingestion
    ingest_processed_total
    ingest_errors_total
    ingest_lag_seconds
    ws_reconnect_attempts_total

Database
    db_health_state
    db_rows_written_total
    db_write_latency_seconds
    db_writer_queue_depth
    db_failed_batches_total
    db_rows_dropped_total

Redis
    redis_enabled_state
    redis_publish_latency_seconds
    redis_publish_failures_total
```

Everything else is mainly there to make deeper diagnosis possible when one of these primary metrics starts looking wrong.