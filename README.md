<div align="left">

# mini-fintickstreams

### A blazingly fast Rust service for high-frequency crypto market-data streaming and storage

![Rust](https://img.shields.io/badge/Rust-2024-000000?style=flat-square&logo=rust&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-TimescaleDB-336791?style=flat-square&logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-E6522C?style=flat-square&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-F46800?style=flat-square&logo=grafana&logoColor=white)
![Status](https://img.shields.io/badge/status-working%20prototype-f59e0b?style=flat-square)

**Exchange data in. Normalized events, TimescaleDB history, runtime diagnostics, and live controls out.**

[Quick Start](#quick-start) · [Runtime API](docs/Runtime%20HTTP%20API.md) · [Grafana](docs/Grafana%20Setup.md) · [Full Documentation](#documentation)

</div>

---

## What Is It?

`mini-fintickstreams` is a fast asynchronous market-data service written in Rust.

It connects to exchange WebSocket and HTTP APIs, converts native payloads into a shared event format, and writes the results into PostgreSQL/TimescaleDB.

The basic pipeline is:

```text
Exchange
    ↓
Rust / Tokio stream worker
    ↓
Typed exchange payload
    ↓
Normalized MarketEvent
    ↓
PostgreSQL / TimescaleDB
```

It also includes:

- runtime stream management through an Axum HTTP API;
- live per-stream configuration knobs;
- PostgreSQL batching and backpressure controls;
- exchange rate-limit handling;
- reconnect and runtime health guards;
- Prometheus metrics;
- a provisioned Grafana dashboard;
- optional Redis Streams publishing;
- local Linux and Kubernetes deployment templates.

This is a **market-data and database service**, not a trading execution engine. It does not place orders.

---

## Runtime Control UI

The service includes a small built-in control panel for starting and stopping streams, checking health, inspecting instruments, viewing limiter budgets, and changing live stream settings.

<table>
  <tr>
    <td width="50%">
      <img src="docs/images/webui%20dashboard.png" alt="mini-fintickstreams runtime dashboard">
    </td>
    <td width="50%">
      <img src="docs/images/webus%20knobs.png" alt="mini-fintickstreams live stream knobs">
    </td>
  </tr>
  <tr>
    <td align="center"><strong>Streams, health, instruments, and rate-limit budgets</strong></td>
    <td align="center"><strong>Live database and Redis settings for each stream</strong></td>
  </tr>
</table>

The UI is useful for demonstrations and quick manual checks, but it is still experimental. The JSON API remains the reliable control interface.

---

## Why I Built It

The original goal was to **bootstrap trading bots, indicators, and models with recent market history**.

A live trading process often cannot start from an empty state. It may need several hours or a full day of trades, order-book updates, funding, or open-interest history before every indicator and model is ready to make decisions.

`mini-fintickstreams` can collect that history and then continue feeding the same pipeline with live data.

It can also be used as a longer-running market-data collector, but storage requirements grow quickly. Collecting every high-volume stream for one active instrument can produce roughly **1–2 GB per day for that single instrument**, depending on market activity, order-book depth, retention, batching, and compression settings.

TimescaleDB retention and compression policies are configurable, so the service can be tuned for either:

- short bootstrapping windows;
- continuous research datasets;
- temporary high-resolution storage;
- longer compressed historical archives.

---

## Monitoring

Streaming infrastructure can remain online while already failing operationally.

A WebSocket may be reconnecting, processing lag may be increasing, PostgreSQL writers may be saturated, queues may be growing, or data may already be getting dropped.

Grafana exists to answer one main question immediately:

> **Is the service actually healthy and keeping up with the data?**

![Grafana runtime dashboard](docs/images/grafana%20dash.png)

The included dashboard shows:

- application and runtime health;
- active streams;
- processed messages per second;
- database rows written per second;
- ingestion lag;
- WebSocket reconnects;
- database write latency;
- writer queue depth;
- failed batches;
- dropped rows;
- optional Redis health.

The dashboard is provisioned from files in `grafana/`, so it can be recreated without manually building every panel.

---

## Current Exchange Support

| Exchange | Market availability | Current status |
|---|---|---|
| **Bybit Linear** | The instrument registry loads the current Bybit Linear perpetual market set | Trades validated end to end with live TimescaleDB writes |
| **Binance Linear** | Configuration, payload types, mapping, and stream code exist | Almost finished, but the live path still needs validation and fixes |
| **Hyperliquid Perp** | Configuration, payload types, mapping, and stream code exist | Almost finished, but not yet fully validated end to end |

Bybit currently provides the working demonstration path. Any Bybit Linear perpetual instrument returned by the registry can be selected when starting a stream.

Support is tracked per **exchange, stream kind, and transport**. A function existing in the code does not automatically mean that the complete live path has been validated.

---

## Performance and Scale

The service uses Rust, Tokio, typed Serde payloads, fixed-point integers, asynchronous SQLx connections, and batched PostgreSQL writes.

It is designed for high-throughput market-data streaming with relatively low runtime overhead.

One database writer path can multiplex data from a large number of symbols. With sensible batching and lower-volume stream types, thousands of symbols can be configured on suitable hardware.

The real limit depends heavily on what is being collected:

```text
Trades
    → relatively manageable

Funding / Open Interest
    → low-frequency and inexpensive

Deep L2 order books
    → extremely high volume

Many liquid markets with full depth
    → storage and database throughput become the real bottleneck
```

Important tuning options include:

- batch size;
- flush interval;
- database chunk size;
- maximum buffered rows;
- PostgreSQL pool size;
- TimescaleDB chunk intervals;
- compression timing;
- retention periods;
- enabled stream types.

Increasing concurrency is not always faster. PostgreSQL has practical writer and connection limits, so batching and backpressure are usually more important than simply adding more writers.

---

## Where to Run It

### Next to Trading Infrastructure

For lower latency and a simpler network path, the service can run on the same Linux server or local network as the trading bots consuming its data.

```text
Exchange
    ↓
mini-fintickstreams
    ↓
local database / trading infrastructure
```

This is the better option when recent data needs to reach local consumers with as few network hops as possible.

### Remote Data-Collection Cluster

For continuous collection, research datasets, monitoring, and centralized storage, the service can also run remotely on Kubernetes.

```text
Exchange
    ↓
mini-fintickstreams Pod
    ↓
TimescaleDB Service
    ↓
Prometheus and Grafana
```

The included Kubernetes files are working templates from the original setup. They still need to be adjusted for the target cluster, image name, namespace, resources, credentials, storage, and Service names.

---

## Quick Start

### Requirements

The normal local setup uses:

- Linux;
- Rust and Cargo;
- PostgreSQL with TimescaleDB;
- `jq` for readable API output;
- Redis only when Redis publishing is enabled;
- Prometheus and Grafana only when monitoring is required.

Follow the [PostgreSQL and TimescaleDB setup guide](docs/PostgreSQL%20and%20TimescaleDB%20Setup.md) before running the application for the first time.

### Download and Build

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Download the project
git clone https://github.com/flowdrivenml/mini-fintickstreams.git
cd mini-fintickstreams

# Build the optimized binary
cargo build --release

# Configure the database connection
export SHARD_MAIN_DSN="postgresql://USER:PASSWORD@127.0.0.1:5432/DATABASE_NAME"

# Create the TimescaleDB schemas and stream registry
psql "$SHARD_MAIN_DSN" -f db/dbsetup.sql
psql "$SHARD_MAIN_DSN" -f db/registry.sql

# Start the application using the local TOML files
./target/release/mini-fintickstreams \
  --config file \
  --stream-version 1 \
  --shutdown-action none
```

Redis is enabled in the current default configuration. Either install it:

```bash
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server
```

or disable it in `src/config/app.toml`:

```toml
[redis]
enabled = false
```

---

## Start a Stream

Start a Bybit BTCUSDT trades stream:

```bash
curl -s -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "BybitLinear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }' | jq
```

Check the active streams and health:

```bash
curl -s http://localhost:8080/streams | jq
curl -s http://localhost:8080/health/runtime | jq
curl -s http://localhost:8080/health/db | jq
```

Check that rows are reaching TimescaleDB:

```bash
psql "$SHARD_MAIN_DSN" -c "
SELECT symbol, COUNT(*), MAX(time)
FROM ex_bybit_linear.trades
GROUP BY symbol
ORDER BY MAX(time) DESC;
"
```

---

## Services

| Service | Address | Purpose |
|---|---|---|
| Runtime API | [http://localhost:8080](http://localhost:8080) | Health, streams, instruments, knobs, and limiters |
| Experimental UI | [http://localhost:8080/ui](http://localhost:8080/ui) | Manual runtime controls |
| Metrics endpoint | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Raw application metrics |
| Prometheus | [http://localhost:9090](http://localhost:9090) | Metric storage and PromQL |
| Grafana | [http://localhost:3000](http://localhost:3000) | Runtime diagnostics dashboard |

---

## Documentation

| Guide | What it covers |
|---|---|
| [PostgreSQL and TimescaleDB Setup](docs/PostgreSQL%20and%20TimescaleDB%20Setup.md) | Database installation, users, `SHARD_MAIN_DSN`, SQL setup, retention, and Kubernetes |
| [Configuration Reference](docs/Configuration%20Reference.md) | TOML files, environment variables, scales, batching, limits, and configuration rough edges |
| [Runtime HTTP API](docs/Runtime%20HTTP%20API.md) | Health checks, stream management, instruments, knobs, and limiters |
| [Prometheus Setup](docs/Prometheus%20Setup.md) | Local and Kubernetes scraping configuration |
| [Prometheus Metrics](docs/Prometheus%20Metrics.md) | Meaning of runtime, ingestion, database, and Redis metrics |
| [Grafana Setup](docs/Grafana%20Setup.md) | Automatic datasource and dashboard provisioning |
| [Redis Setup](docs/Redis%20Setup.md) | Optional Redis Streams publishing and its latency limitations |
| [Adding and Supporting a New Exchange](docs/Adding%20and%20Supporting%20a%20New%20Exchange.md) | Exchange integration architecture, current limitations, and future refactoring |

---

## Project Status

This is a working engineering prototype built around:

**Rust · algorithmic trading infrastructure · high-frequency market-data streaming · PostgreSQL · TimescaleDB · Prometheus · Grafana · Kubernetes**

The Bybit trade pipeline is validated from the exchange WebSocket through normalized Rust events and into TimescaleDB.

The exchange integration layer was built while I was still learning Rust, so some modules are more fragmented and duplicated than they should be. Binance and Hyperliquid are close, but they still need a proper validation pass before being presented as finished integrations.

The immediate priority is keeping the working pipeline stable, observable, configurable, and honestly documented.
