
This document explains the TOML configuration files under:

```text
src/config/
```

The configuration system is **messy but working**. It grew one subsystem at a time, so settings are split across several TOML files, path selection is controlled separately, and Kubernetes contains another copy of the same configuration.

The current layout is usable, but it is not yet a clean, unified configuration system.

## Quick Navigation

- [[#Configuration Overview]]
- [[#How Configuration Is Loaded]]
  - [[#Local File Mode]]
  - [[#Environment Path Mode]]
  - [[#Configuration Version Selector]]
- [[#Required Environment Variables]]
- [[#Configuration Files]]
  - [[#app.toml]]
  - [[#api.toml]]
  - [[#binance_linear.toml]]
  - [[#bybit_linear.toml]]
  - [[#hyperliquid_perp.toml]]
  - [[#timescale_db.toml]]
  - [[#redis.toml]]
  - [[#prometheus.toml]]
- [[#Kubernetes Configuration]]
- [[#Known Rough Edges]]
- [[#Safe Editing Checklist]]

---

## Configuration Overview

The project currently contains eight TOML files:

```text
src/config/
├── api.toml
├── app.toml
├── binance_linear.toml
├── bybit_linear.toml
├── hyperliquid_perp.toml
├── prometheus.toml
├── redis.toml
└── timescale_db.toml
```

Each file owns one part of the application:

| File | Responsibility |
|---|---|
| `app.toml` | Main application switches, limits, scaling, reconnect policy, and runtime health |
| `api.toml` | HTTP API address and port |
| `binance_linear.toml` | Binance REST and WebSocket definitions |
| `bybit_linear.toml` | Bybit REST and WebSocket definitions |
| `hyperliquid_perp.toml` | Hyperliquid REST and WebSocket definitions |
| `timescale_db.toml` | PostgreSQL/TimescaleDB shards, pools, batching, and DB health |
| `redis.toml` | Optional Redis publishing, connection, retention, and failover |
| `prometheus.toml` | Application metrics HTTP endpoint and related metric settings |

A useful mental model is:

```text
app.toml
    └── decides which major systems are enabled

exchange TOMLs
    └── explain how to connect and subscribe to exchanges

timescale_db.toml
    └── controls durable database writes

redis.toml
    └── controls optional real-time Redis publishing

api.toml
    └── exposes the runtime control API

prometheus.toml
    └── exposes operational metrics
```

The configuration is read and validated when the process starts. Most TOML changes therefore require an application restart.

Live per-stream knobs are the main exception. They can update selected DB and Redis behavior through the HTTP API without restarting the service.

---

## How Configuration Is Loaded

The application supports two configuration modes:

```text
--config file
--config env
```

The CLI currently defaults to:

```text
--config env
```

This matters when running locally. Starting the application without specifying a mode may make it look under:

```text
/etc/mini-fintickstreams/
```

instead of using the TOML files in the repository.

---

### Local File Mode

Use file mode for normal local development:

```bash
cargo run --release -- \
  --config file \
  --stream-version 1 \
  --shutdown-action none
```

In this mode, the application reads the TOML files from:

```text
src/config/
```

Run the command from the project repository so the relative paths resolve correctly.

The local path is approximately:

```text
mini-fintickstreams
        │
        ▼
src/config/*.toml
        │
        ▼
application config structs
        │
        ▼
runtime
```

---

### Environment Path Mode

Environment mode does **not** mean that every configuration value is stored directly in an environment variable.

Instead, environment variables tell the application **where each TOML file is located**.

For version `1`, the expected variables are:

```text
MINI_FINTICKSTREAMS_APP_CONFIG_PATH_1
MINI_FINTICKSTREAMS_API_CONFIG_PATH_1
MINI_FINTICKSTREAMS_TIMESCALE_CONFIG_PATH_1
MINI_FINTICKSTREAMS_PROMETHEUS_CONFIG_PATH_1
MINI_FINTICKSTREAMS_REDIS_CONFIG_PATH_1

MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_BINANCE_LINEAR
MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_BYBIT_LINEAR
MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_HYPERLIQUID_PERP
```

When one of these variables is missing, the loader normally falls back to:

```text
/etc/mini-fintickstreams/
```

A full local environment-mode setup can look like this:

```bash
export CONFIG_DIR="/absolute/path/to/mini-fintickstreams/src/config"

export MINI_FINTICKSTREAMS_APP_CONFIG_PATH_1="$CONFIG_DIR/app.toml"
export MINI_FINTICKSTREAMS_API_CONFIG_PATH_1="$CONFIG_DIR/api.toml"
export MINI_FINTICKSTREAMS_TIMESCALE_CONFIG_PATH_1="$CONFIG_DIR/timescale_db.toml"
export MINI_FINTICKSTREAMS_PROMETHEUS_CONFIG_PATH_1="$CONFIG_DIR/prometheus.toml"
export MINI_FINTICKSTREAMS_REDIS_CONFIG_PATH_1="$CONFIG_DIR/redis.toml"

export MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_BINANCE_LINEAR="$CONFIG_DIR/binance_linear.toml"
export MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_BYBIT_LINEAR="$CONFIG_DIR/bybit_linear.toml"
export MINI_FINTICKSTREAMS_EXCHANGE_CONFIG_PATH_1_HYPERLIQUID_PERP="$CONFIG_DIR/hyperliquid_perp.toml"

./target/release/mini-fintickstreams \
  --config env \
  --stream-version 1 \
  --shutdown-action none
```

Environment mode is mainly intended for:

```text
Docker
Kubernetes
systemd deployments
custom configuration directories
```

---

### Configuration Version Selector

The command-line option:

```text
--stream-version 1
```

selects the suffix used by the configuration path variables.

For example:

```text
--stream-version 1
    ↓
MINI_FINTICKSTREAMS_APP_CONFIG_PATH_1

--stream-version 2
    ↓
MINI_FINTICKSTREAMS_APP_CONFIG_PATH_2
```

This is separate from:

```toml
config_version = 1
```

inside `app.toml`.

The two values currently have different jobs:

```text
--stream-version
    └── selects versioned environment-variable names

app.toml config_version
    └── application identity and metric metadata
```

Changing `app.toml` to:

```toml
config_version = 2
```

does not automatically make the loader search for variables ending in `_2`.

That behavior is controlled by:

```text
--stream-version 2
```

---

## Required Environment Variables

Some values are intentionally kept outside the TOML files because they contain credentials or deployment-specific addresses.

### PostgreSQL DSN

`timescale_db.toml` contains:

```toml
dsn_env = "SHARD_MAIN_DSN"
```

This means the actual connection string must be available as:

```bash
export SHARD_MAIN_DSN="postgresql://USERNAME:PASSWORD@HOST:5432/DATABASE"
```

For example:

```bash
export SHARD_MAIN_DSN="postgresql://fintick:password@127.0.0.1:5432/fintickstreams"
```

The TOML file stores only the **name of the environment variable**, not the password or DSN itself.

If DB support is enabled and `SHARD_MAIN_DSN` is missing, configuration validation fails during startup.

### Redis URI

`redis.toml` contains:

```toml
[nodes]
a = "redis://127.0.0.1:6379"

[nodes_env]
a = "REDIS_NODE_A"
```

In file mode, the application normally uses the URI from `[nodes]`.

In environment mode, it first checks:

```text
REDIS_NODE_A
```

Set it with:

```bash
export REDIS_NODE_A="redis://127.0.0.1:6379"
```

In Kubernetes, it will usually be:

```text
redis://redis:6379
```

If `REDIS_NODE_A` is absent, the Redis loader can fall back to the URI in `[nodes]`. That fallback is useful locally but usually wrong in Kubernetes because `127.0.0.1` would mean the application Pod itself.

---

## Configuration Files

## `app.toml`

`app.toml` is the main application configuration.

It decides which major dependencies and exchanges are enabled, defines fixed-point scaling, configures reconnect behavior, and sets runtime health thresholds.

### Application Identity

```toml
id = "market-ingest"
env = "prod"
config_version = 1
```

These values identify the service and appear in application metrics.

They do not configure exchange access or database credentials.

### Database Switch

```toml
[db]
enabled = true
verify = true
```

| Setting | Meaning |
|---|---|
| `enabled` | Creates the DB pools and enables database writing |
| `verify` | Runs a connectivity check during startup |

The current validation requires:

```text
db.enabled = true
    → db.verify must also be true
```

When DB support is disabled at startup, the application uses a no-op DB writer rather than constructing the real database dependency.

### Redis Switch

```toml
[redis]
enabled = true
```

This is the actual application-level Redis switch.

When false:

```text
Redis config is not loaded
Redis client is not created
publishing uses a no-op implementation
database writing can continue
```

There is also a root-level value in the current file:

```toml
redis_enabled = true
```

That root-level field is not represented by the current `AppConfig` structure. Treat it as a legacy leftover.

Use this setting instead:

```toml
[redis]
enabled = true
```

### Fixed-Point Scales

```toml
[scales]
price = 100000000
qty = 100000000
open_interest = 100000000
funding = 1000000000000
```

Market values are converted to integers before storage:

```text
stored_value = real_value × scale
```

This avoids using floating-point values in the database. Values are converted to fixed-point integers before storage to keep high-volume market data compact, exact, and efficient to process.

These settings are important because changing them changes the meaning of stored integers.

For example:

```text
price_i = 100000000000

scale = 100000000
real price = 1000
```

Changing the scale later without migrating existing data would make historical rows inconsistent.

Treat scale changes as a data-format migration, not as normal tuning.

Be careful when choosing scales for very small-priced or very small-quantity instruments. Some markets use values many orders of magnitude smaller than typical assets. If the configured scale does not provide enough precision, converting the value to an integer can round it to `0` or otherwise lose meaningful precision.

For example, with a scale of `1e8`, a value smaller than the representable precision can collapse during conversion. Before adding new exchanges or unusual instruments, verify that the configured price and quantity scales can represent their minimum tick size and quantity step safely.

The opposite problem also exists: making the scale unnecessarily large increases the integer magnitude and can eventually create `i64` overflow risk. The scale therefore needs to cover the **smallest values you expect without exceeding the integer range for the largest values you expect**.

### Exchange Toggles

```toml
[exchange_toggles]
binance_linear = true
hyperliquid_perp = true
bybit_linear = true
```

The application loads an exchange TOML only when its toggle is enabled.

For example:

```text
bybit_linear = false
    ↓
bybit_linear.toml is not loaded
    ↓
Bybit HTTP and WebSocket clients are not created
```

Disabling an unused or broken exchange here is cleaner than deleting its configuration file.

### WebSocket Reconnect Policy

```toml
[streams]
ws_reconnect_backoff_initial_ms = 500
ws_reconnect_backoff_max_ms = 30000
ws_reconnect_trip_after_failures = 10
ws_reconnect_cooldown_seconds = 120
```

These are global reconnect and circuit-breaker-style settings.

| Setting | Meaning |
|---|---|
| `ws_reconnect_backoff_initial_ms` | First reconnect delay |
| `ws_reconnect_backoff_max_ms` | Maximum reconnect delay |
| `ws_reconnect_trip_after_failures` | Consecutive failures before the connection is temporarily tripped |
| `ws_reconnect_cooldown_seconds` | Cooldown before trying again |

Exchange-specific reconnect budgets are configured separately in each exchange TOML.

### Limits

```toml
[limits]
max_active_streams = 500
max_events_per_sec = 500000
```

These describe intended application capacity.

`max_active_streams` is also exposed through application metrics as the configured stream limit.

Some limit fields are currently ahead of the implementation. In particular, do not assume that editing `max_events_per_sec` automatically creates a fully enforced global event-rate limiter.

### Logging and Metrics Switches

```toml
[logging]
level = "info"

[metrics]
enabled = true
```

These fields are present in the application configuration, but the current runtime wiring is incomplete.

The tracing setup and Prometheus server have their own initialization paths, so changing these fields may not currently change all logging or metric behavior.

Treat them as intended configuration rather than fully authoritative switches until the wiring is cleaned up.

### Runtime Health

```toml
[health]
enabled = true

[health.runtime]
enabled = true
poll_interval_ms = 1000
hold_down_ms = 3000

max_rss_mb_red = 14000
min_avail_mem_mb_red = 800
fd_pct_red = 90

tick_drift_ms_red = 200
drift_sustain_ticks = 5

cpu_pct_red = 95
cpu_sustain_sec = 10
```

This protects the process from accepting more work while it is overloaded.

The runtime checks:

```text
process RAM
available system RAM
file descriptor usage
Tokio scheduler drift
sustained CPU usage
```

When a threshold remains unhealthy for the configured hold-down period, runtime health becomes RED and new stream admission can be blocked.

These thresholds depend heavily on the machine or Pod limits. The current `14 GB` RSS threshold may make sense on a large local machine but not inside a Kubernetes Pod limited to `2 GiB`.

---

## `api.toml`

`api.toml` controls the Axum HTTP API server:

```toml
bind_addr = "0.0.0.0"
port = 8080
```

| Setting | Meaning |
|---|---|
| `bind_addr` | Network interface on which the API listens |
| `port` | HTTP API port |

With the default settings, the API is available locally at:

```text
http://localhost:8080
```

Using:

```text
0.0.0.0
```

allows connections through Docker port mappings and Kubernetes Services.

Using:

```text
127.0.0.1
```

would make the server reachable only from the same network namespace.

---

## Exchange Configuration Files

The three exchange TOMLs share the same general purpose:

```text
REST API definitions
WebSocket definitions
rate-limiter budgets
heartbeat behavior
reconnect budgets
subscription templates
endpoint templates
stream-name templates
```

Common top-level settings include:

| Setting | Meaning |
|---|---|
| `timezone` | Expected exchange timestamp timezone |
| `exchange` | Native exchange name |
| `margin` | Market or margin type |
| `api_pool` | Internal rate-limiter pool identifier |
| `api_base_url` | REST API base URL |
| `max_weight` | Request budget for the configured window |
| `window` | Rate-limit window in seconds |
| `ws_base_url` | WebSocket server |
| `ws_connection_timeout_seconds` | Maximum connection lifetime, where used |
| `ws_max_streams_per_connection` | Intended subscription capacity per connection |
| `ws_heartbeat_*` | Heartbeat behavior |
| `ws_reconnect_*` | Reconnect admission budget |
| `ws_subscribe_*` | Subscription admission budget |

Dynamic placeholders are rendered when a request or stream is created:

```text
<symbol>
<coin>
<stream_title>
<stream_id>
<subscription_type>
```

Do not remove or rename placeholders unless the corresponding rendering code is updated.

---

## `binance_linear.toml`

This file describes Binance USDT-M Futures.

Important defaults include:

```text
REST:      https://fapi.binance.com
WebSocket: wss://fstream.binance.com/ws
```

The file defines REST endpoints such as:

```text
ping
server time
exchange information
depth snapshot
open interest
funding rate
```

It also defines WebSocket stream templates:

```text
<symbol>@depth@100ms
<symbol>@aggTrade
<symbol>@forceOrder
```

The application replaces `<symbol>` before subscribing.

For Binance, symbols used inside stream names are normally normalized by the application before the template is rendered.

The configured API weights are used by the client-side limiter. They should be reviewed whenever Binance changes endpoint weights or rate-limit rules.

---

## `bybit_linear.toml`

This file describes Bybit V5 public linear derivatives.

Important defaults include:

```text
REST:      https://api.bybit.com
WebSocket: wss://stream.bybit.com/v5/public/linear
```

Bybit also provides dynamic rate-limit information through response headers:

```text
x-bapi-limit-status
x-bapi-limit
x-bapi-limit-reset-timestamp
```

WebSocket stream templates include:

```text
orderbook.1000.<symbol>
publicTrade.<symbol>
allLiquidation.<symbol>
tickers.<symbol>
```

The `tickers.<symbol>` stream is used as a combined source for funding and open-interest updates.

Bybit subscription messages are JSON objects:

```toml
ws_subscribe_msg = {
  req_id = "<stream_id>",
  op = "subscribe",
  args = ["<stream_title>"]
}
```

This is different from Binance and Hyperliquid, which is why each exchange keeps its own configuration file.

---

## `hyperliquid_perp.toml`

This file describes Hyperliquid perpetual markets.

Important defaults include:

```text
REST:      https://api.hyperliquid.xyz
WebSocket: wss://api.hyperliquid.xyz/ws
```

Hyperliquid uses:

```text
POST /info
```

for several REST operations.

Its WebSocket subscriptions are structured objects rather than simple stream strings:

```toml
ws_subscribe_msg = {
  method = "subscribe",
  subscription = {
    type = "<subscription_type>",
    coin = "<coin>"
  }
}
```

Configured subscription types include:

```text
l2Book
trades
activeAssetCtx
```

`activeAssetCtx` provides the combined funding and open-interest stream.

The rate limits in this file are client-side safety limits because the file notes that Hyperliquid does not publish the same kind of official weight model used by Binance.

---

## `timescale_db.toml`

This file controls PostgreSQL/TimescaleDB connections, routing, batching, and database health.

### Shards

The current setup has one shard:

```toml
[[shards]]
id = "shard0"
dsn_env = "SHARD_MAIN_DSN"
```

The actual database URL is loaded from the environment variable named by `dsn_env`.

This makes future multi-database configurations possible without placing passwords in TOML.

### Connection Pool

```toml
pool_min = 1
pool_max = 10
connect_timeout_ms = 5000
idle_timeout_sec = 300
```

`pool_max` is particularly important. PostgreSQL has limited connection and concurrent-write capacity, so increasing it does not automatically improve throughput.

Too much concurrency can increase:

```text
pool wait
lock pressure
disk pressure
write latency
```

### Shard Routing

```toml
[[shards.rules]]
exchange = "*"
stream = "*"
symbol = "*"
```

The wildcard rule sends all data to `shard0`.

Future rules can route a subset of data to another database:

```toml
[[shards.rules]]
exchange = "bybit_linear"
stream = "depth"
symbol = "*"
```

The routing code prefers the most specific matching rule.

### Writer Defaults

```toml
[writer]
batch_size = 1000
hard_batch_size = 10000
flush_interval_ms = 50
chunk_rows = 5000
max_inflight_batches = 4
use_copy = true
```

| Setting | Meaning |
|---|---|
| `batch_size` | Row count that can trigger a flush |
| `hard_batch_size` | Maximum rows allowed in one in-memory batch |
| `flush_interval_ms` | Maximum time rows wait before becoming flushable |
| `chunk_rows` | Maximum rows sent in one DB insert chunk |
| `max_inflight_batches` | Concurrent writer/backpressure limit |
| `use_copy` | Selects the intended bulk-write strategy |

These values become the initial defaults for new stream batches.

Selected values can later be changed per stream through runtime knobs:

```text
flush_rows
flush_interval_ms
chunk_rows
hard_cap_rows
```

### Database Health

```toml
[health]
enabled = true
evaluate_interval_ms = 1000
hold_down_ms = 3000
admission_policy = "green_only"
```

The thresholds watch:

```text
flush delay
connection-pool wait
writer queue depth
```

The database can report:

```text
GREEN
YELLOW
RED
```

With:

```text
admission_policy = "green_only"
```

new streams are accepted only while DB health is GREEN.

---

## `redis.toml`

This file controls the optional Redis producer.

The main application switch still lives in:

```toml
# app.toml
[redis]
enabled = true
```

When that switch is enabled, `redis.toml` defines how Redis should be used.

### Node Selection

```toml
mode = "single"
default_node = "a"

[nodes]
a = "redis://127.0.0.1:6379"

[nodes_env]
a = "REDIS_NODE_A"
```

Current production behavior is designed around one Redis node.

A future pool mode exists in the configuration model, but the current comments and implementation make it clear that multi-node Redis support is incomplete.

### Connection Behavior

```toml
[connection]
connect_timeout_ms = 2000
command_timeout_ms = 2000
keepalive_sec = 30
tcp_nodelay = true
```

These settings control connection and command timeouts.

`tcp_nodelay` disables Nagle-style buffering and is appropriate for small latency-sensitive commands.

### Capacity Guardrails

```toml
[capacity]
poll_interval_sec = 2
max_memory_pct = 85
max_pending = 200000
max_p99_cmd_ms = 10
redis_publish_latency_window = 2048
```

These values determine when Redis is considered too slow or too saturated to remain in the active publishing path.

### Failure Policy

```toml
[failover]
on_saturated = "stop_assigning_new"
on_down = "disable_redis_temporarily"
```

Redis is best-effort.

When Redis becomes unhealthy:

```text
new Redis assignments may stop
Redis publishing may be disabled
database writing continues
```

### Published Streams

```toml
[streams]
key_format = "stream:{exchange}:{symbol}:{kind}"

publish_trades = true
publish_depth = true
publish_liquidations = true
publish_funding = true
publish_open_interest = true
```

These switches decide which normalized event types are published to Redis Streams.

### Retention

```toml
[retention]
maxlen = 5000
approx = true
```

Redis keeps approximately the latest `5,000` entries per stream key.

This is intended as a short-lived live-data buffer, not durable history.

### Consumer Groups

```toml
[groups]
feature_builder = "cg:features"
```

This section is documentation for downstream consumers.

`mini-fintickstreams` publishes events but does not currently consume them through this group.

---

## `prometheus.toml`

This file configures the HTTP endpoint that exposes application metrics.

The settings that clearly drive the current metrics server are:

```toml
bind_addr = "0.0.0.0"
port = 8000
metrics_path = "/metrics"
```

The resulting endpoint is:

```text
http://localhost:8000/metrics
```

The file also contains:

```text
labels
export switches
Redis polling options
reference scrape targets
```

These sections are parsed and validated, but much of the current runtime does not yet use them to control metric registration.

In particular:

```toml
[targets]
```

does **not** configure the external Prometheus service.

Prometheus scraping must still be configured in:

```text
/etc/prometheus/prometheus.yml
```

or through Kubernetes Prometheus resources.

The current metrics server mainly uses:

```text
bind_addr
port
metrics_path
```

The remaining fields are partly documentation and partly planned configuration.

---

## Kubernetes Configuration

Kubernetes uses the same TOML data, but the files are mounted into the container under:

```text
/etc/mini-fintickstreams/
```

The Deployment runs the application using:

```text
--config env
--stream-version 1
```

and points the versioned path variables to the mounted files.

The architecture is:

```text
src/config/*.toml
        │
        │ copied into ConfigMap
        ▼
mini-fintickstreams-config
        │
        │ mounted read-only
        ▼
/etc/mini-fintickstreams/*.toml
        │
        ▼
application Pod
```

Secrets are kept separately:

```text
SHARD_MAIN_DSN
REDIS_NODE_A
```

### Generate the ConfigMap from `src/config`

The checked-in `k8s/configmap.yaml` manually duplicates all TOML content. That is easy to forget and easy to break.

A safer approach is to generate the ConfigMap directly from the configuration directory:

```bash
export NS="mini-fintickstreams"
export PROJECT_ROOT="/absolute/path/to/mini-fintickstreams"
export CONFIG_DIR="$PROJECT_ROOT/src/config"

kubectl create namespace "$NS" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -

kubectl -n "$NS" create configmap \
  mini-fintickstreams-config \
  --from-file="$CONFIG_DIR" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

This automatically creates one ConfigMap key for every TOML file in `src/config`.

### Create the Secret

Do not place real passwords in `k8s/secret.yaml` before committing it.

Create or update the Secret directly:

```bash
kubectl -n "$NS" create secret generic \
  mini-fintickstreams-secrets \
  --from-literal=SHARD_MAIN_DSN="postgresql://USER:PASSWORD@POSTGRES_SERVICE:5432/DATABASE" \
  --from-literal=REDIS_NODE_A="redis://redis:6379" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Then apply or restart the application:

```bash
kubectl -n "$NS" apply -f k8s/deployment.yaml
kubectl -n "$NS" apply -f k8s/service.yaml

kubectl -n "$NS" rollout restart \
  deployment/mini-fintickstreams

kubectl -n "$NS" rollout status \
  deployment/mini-fintickstreams
```

The application does not currently hot-reload all TOML configuration, so restart the Pod after changing the ConfigMap.

### Current ConfigMap Warning

The current checked-in `k8s/configmap.yaml` has configuration drift:

```text
app.toml copy
    └── missing bybit_linear in [exchange_toggles]

later exchange block
    └── Bybit content is labeled as binance_linear.toml
```

The Deployment expects:

```text
/etc/mini-fintickstreams/bybit_linear.toml
```

Before using the checked-in ConfigMap directly, synchronize it with:

```text
src/config/
```

Generating it with `kubectl --from-file` avoids this class of mistake.

---

## Known Rough Edges

The current configuration works, but these parts should eventually be cleaned up.

### Configuration Is Split Across Too Many Places

The effective runtime configuration is currently spread across:

```text
src/config/*.toml
environment variables
CLI arguments
k8s/configmap.yaml
k8s/secret.yaml
Dockerfile defaults
runtime stream knobs
```

That makes it possible for two settings to look related while controlling different things.

### `redis_enabled` Is Duplicated

The root setting:

```toml
redis_enabled = true
```

is a legacy-looking value.

The runtime uses:

```toml
[redis]
enabled = true
```

The duplicate root value should eventually be removed after confirming no external tooling depends on it.

### Configuration Version Naming Is Confusing

These are different:

```text
app.toml config_version
--stream-version
MINI_FINTICKSTREAMS_*_CONFIG_PATH_1
```

The Dockerfile also defines:

```text
MINI_FINTICKSTREAMS_CONFIG_VERSION
```

but the current process selects path suffixes using the CLI `--stream-version` value.

This should eventually become one clear configuration-version mechanism.

### Some Fields Are Not Fully Wired

Several fields are parsed and validated but are not yet authoritative runtime controls.

Examples include parts of:

```text
app.toml [logging]
app.toml [metrics]
app.toml limits.max_events_per_sec
prometheus.toml [export]
prometheus.toml [redis_poll]
prometheus.toml [targets]
```

Keep them because the files currently expect them, but verify actual behavior before relying on them operationally.

### Kubernetes Duplicates the TOML Files

Maintaining the same configuration in both:

```text
src/config/*.toml
k8s/configmap.yaml
```

creates two sources of truth.

Generating the ConfigMap from `src/config` is safer than manually copying every setting.

---

## Safe Editing Checklist

Before changing configuration:

- Confirm whether the process uses `--config file` or `--config env`.
- Confirm the active `--stream-version`.
- Do not put passwords directly in TOML files.
- Do not casually change fixed-point scales after data has been stored.
- Keep exchange template placeholders intact.
- Review exchange rate limits when endpoint definitions change.
- Keep DB pool and writer concurrency conservative.
- Match runtime health thresholds to the actual machine or Pod limits.
- Restart the application after TOML changes.
- Verify all health endpoints after restarting.

Local verification:

```bash
curl -s http://localhost:8080/health/runtime | jq
curl -s http://localhost:8080/health/db | jq
curl -s http://localhost:8080/health/redis | jq
curl -s http://localhost:8000/metrics | head
```

Kubernetes verification:

```bash
kubectl -n mini-fintickstreams get pods

kubectl -n mini-fintickstreams logs \
  deployment/mini-fintickstreams \
  --tail=200

kubectl -n mini-fintickstreams port-forward \
  service/mini-fintickstreams \
  8080:8080
```

Then:

```bash
curl -s http://localhost:8080/health/runtime | jq
```

If the application fails during startup, configuration errors usually identify:

```text
which file was loaded
which path was attempted
which environment variable selected it
which value failed validation
```

The configuration system is not elegant yet, but it does fail early and gives reasonably useful diagnostics.