<div align="center">

# mini-fintickstreams

**Live crypto market data in. Normalized events and TimescaleDB history out.**

![Rust](https://img.shields.io/badge/Rust-2024-000000?style=flat-square&logo=rust&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Linux-2563eb?style=flat-square&logo=linux&logoColor=white)
![Status](https://img.shields.io/badge/status-working%20prototype-f59e0b?style=flat-square)
![Validated](https://img.shields.io/badge/validated-Bybit%20Linear-22c55e?style=flat-square)

A Linux-first Rust service for collecting exchange data, normalizing it, storing it in PostgreSQL/TimescaleDB, and monitoring the full pipeline in Grafana.

[Quick Start](#quick-start) · [Runtime API](docs/Runtime%20HTTP%20API.md) · [Grafana](docs/Grafana%20Setup.md) · [Documentation](#documentation)

</div>

![Grafana runtime dashboard](docs/images/grafana%20dash.png)

## What Is This?

`mini-fintickstreams` runs exchange WebSocket and HTTP streams as independent async tasks.

Incoming payloads are parsed into typed Rust structures, converted into shared market events, and written in batches to TimescaleDB. The application also exposes an HTTP control API, runtime health checks, Prometheus metrics, configurable stream knobs, and optional Redis publishing.

The basic path is:

**Exchange → Tokio stream worker → normalized `MarketEvent` → TimescaleDB → Prometheus → Grafana**

The project was mainly built to provide recent market history for bootstrapping trading indicators and models, then continue feeding them with live data. It does **not** place trades.

> **Current status:** Bybit Linear trades are the only live path currently validated end to end. Binance Linear and Hyperliquid Perp code exists, but those integrations should still be treated as incomplete or unvalidated.

## What Is Included?

| Area | What the project provides |
|---|---|
| Ingestion | Async WebSocket connections and HTTP polling |
| Normalization | Typed exchange payloads converted into shared market events |
| Storage | Batched PostgreSQL/TimescaleDB writes |
| Runtime control | Start, stop, inspect, and tune streams over HTTP |
| Safety | Reconnect backoff, rate limiting, health gates, and queue limits |
| Observability | Prometheus metrics and a provisioned Grafana dashboard |
| Deployment | Local Linux setup, Dockerfile, and Kubernetes templates |
| Live delivery | Optional Redis Streams publishing |

## Quick Start

This path assumes PostgreSQL and TimescaleDB are already installed. Follow the [database setup guide](docs/PostgreSQL%20and%20TimescaleDB%20Setup.md) first if they are not.

Create your PostgreSQL user and database, then run the following from the project root:

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Download the project
git clone <paste-the-HTTPS-repository-url-here>
cd mini-fintickstreams

# Build the release binary
cargo build --release

# Point the application at your database
export SHARD_MAIN_DSN="postgresql://USER:PASSWORD@127.0.0.1:5432/DATABASE_NAME"

# Create the TimescaleDB tables and stream registry
psql "$SHARD_MAIN_DSN" -f db/dbsetup.sql
psql "$SHARD_MAIN_DSN" -f db/registry.sql

# Redis is optional, but currently enabled in the default configuration
sudo apt update
sudo apt install -y redis-server
sudo systemctl enable --now redis-server

# Run with the TOML files from src/config/
./target/release/mini-fintickstreams \
  --config file \
  --stream-version 1 \
  --shutdown-action none
```

To run without Redis, change this in `src/config/app.toml` before starting:

```toml
[redis]
enabled = false
```

### Start a Bybit Trade Stream

Once the application is running:

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

Verify that it is active:

```bash
curl -s http://localhost:8080/streams | jq
curl -s http://localhost:8080/health/runtime | jq
curl -s http://localhost:8080/health/db | jq
```

## Open the Services

| Service | Address | Purpose |
|---|---|---|
| Runtime API | [http://localhost:8080](http://localhost:8080) | Stream control and health checks |
| Built-in UI | [http://localhost:8080/ui](http://localhost:8080/ui) | Experimental manual control panel |
| Application metrics | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Raw Prometheus metrics |
| Prometheus | [http://localhost:9090](http://localhost:9090) | Metric storage and PromQL |
| Grafana | [http://localhost:3000](http://localhost:3000) | Provisioned monitoring dashboard |

The built-in UI is useful for demonstrations and quick checks, but it is still experimental and contains known bugs. Use the JSON API for reliable automation.

## Runtime Controls

<table>
  <tr>
    <td width="50%">
      <img src="docs/images/webui%20dashboard.png" alt="Built-in runtime dashboard">
    </td>
    <td width="50%">
      <img src="docs/images/webus%20knobs.png" alt="Live stream knobs">
    </td>
  </tr>
  <tr>
    <td align="center"><strong>Streams, health, instruments, and limiter budgets</strong></td>
    <td align="center"><strong>Live database and Redis stream settings</strong></td>
  </tr>
</table>

Stream knobs can change settings such as batch size, flush interval, database writes, and Redis publishing without restarting the stream.

## Documentation

| Guide | Start here when you need to... |
|---|---|
| [PostgreSQL and TimescaleDB Setup](docs/PostgreSQL%20and%20TimescaleDB%20Setup.md) | Install the database, create a user, configure `SHARD_MAIN_DSN`, and run the SQL setup |
| [Configuration Reference](docs/Configuration%20Reference.md) | Understand the TOML files, environment variables, scales, limits, and Kubernetes configuration |
| [Runtime HTTP API](docs/Runtime%20HTTP%20API.md) | Start, stop, inspect, and tune streams |
| [Prometheus Setup](docs/Prometheus%20Setup.md) | Configure Prometheus to scrape the application |
| [Prometheus Metrics](docs/Prometheus%20Metrics.md) | Understand the custom runtime, ingest, DB, and Redis metrics |
| [Grafana Setup](docs/Grafana%20Setup.md) | Load the included dashboard automatically |
| [Redis Setup](docs/Redis%20Setup.md) | Configure optional Redis Streams publishing |
| [Adding and Supporting a New Exchange](docs/Adding%20and%20Supporting%20a%20New%20Exchange.md) | Understand the exchange architecture, limitations, and extension process |

## Project Note

This is a working engineering prototype, not a finished exchange plug-in framework.

Most of the exchange layer was built while I was still learning Rust, so some module boundaries are fragmented and several stream workers contain duplicated orchestration. The current priority is keeping the validated Bybit path stable and documenting the system honestly rather than rushing a risky full rewrite.