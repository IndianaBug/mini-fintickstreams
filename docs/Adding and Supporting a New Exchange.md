## Quick Navigation

- [Current Support Status](#current-support-status)
- [Important Design Note](#important-design-note)
- [Why Rust Was Used](#why-rust-was-used)
- [How Exchange Data Moves Through the System](#how-exchange-data-moves-through-the-system)
- [Why There Is No Universal Exchange Adapter](#why-there-is-no-universal-exchange-adapter)
- [What Is Reusable](#what-is-reusable)
- [What Is Currently Messy](#what-is-currently-messy)
- [Adding an Exchange in the Current Codebase](#adding-an-exchange-in-the-current-codebase)
- [Validation Requirements](#validation-requirements)
- [Future Refactor Direction](#future-refactor-direction)
- [Practical Conclusion](#practical-conclusion)

---

## Current Support Status

> [!warning] Current state
> **Bybit Linear is currently the only exchange validated as a working end-to-end path.**

The Bybit trades stream has been tested with live market data and confirmed to write rows into PostgreSQL/TimescaleDB.

The repository also contains configuration, payload types, mapping code, routing, and partial stream implementations for:

```text
Binance Linear
Hyperliquid Perp
```

However, those integrations should currently be treated as **incomplete or unvalidated**, not as production-ready exchange support.

A useful distinction is:

| Exchange | Current status |
|---|---|
| Bybit Linear | Working demonstration path; trades validated end to end |
| Binance Linear | Integration code exists, but the live path is not currently reliable |
| Hyperliquid Perp | Integration code exists, but it has not been fully validated end to end |

A stream kind should also not be considered supported simply because its function exists. Trades, order books, liquidations, funding, and open interest each need their own live validation.

---

## Important Design Note

I built most of the exchange layer while I was still learning Rust.

Because of that, I made several module-design decisions that I would not make in the same way today. I focused first on getting real exchange data through the complete pipeline, and only later became experienced enough with Rust to see where the boundaries should have been cleaner.

The result is a **working but fragmented prototype**:

- exchange-specific logic is spread across several modules;
- similar WebSocket workers are duplicated;
- stream routing contains large exchange-specific match branches;
- transport, mapping, orchestration, and persistence are sometimes mixed together;
- adding an exchange requires editing more files than it should;
- the configuration layer looks more generic than the runtime implementation actually is.

This is not presented as a finished exchange-plugin framework.

It is a functional engineering prototype that proves the complete path from exchange APIs to normalized events, runtime controls, monitoring, Redis publishing, and TimescaleDB storage.

I currently do not have enough free time to safely refactor the entire project. A proper refactor would affect startup, configuration, WebSocket handling, REST polling, payload mapping, instruments, stream routing, tests, persistence, and Kubernetes configuration.

A rushed partial rewrite could leave the project in a worse intermediate state. For now, the working Bybit path is preserved and the architectural limitations are documented honestly.

---

## Why Rust Was Used

Rust was chosen because this service is a long-running, concurrent market-data process.

It needs to maintain multiple network connections, process a continuous flow of messages, normalize numeric data, write batches to a database, expose metrics, and shut streams down without corrupting shared state.

### Tokio

Tokio provides the asynchronous runtime.

It is used for:

- concurrent WebSocket connections;
- HTTP polling loops;
- timers and reconnect delays;
- background health checks;
- task cancellation;
- database operations;
- graceful shutdown.

Async Rust allows many streams to wait on network traffic without assigning one operating-system thread to every connection.

### `tokio-tungstenite`

`tokio-tungstenite` provides the WebSocket transport.

The shared WebSocket client handles much of the generic connection lifecycle:

- connecting;
- sending subscriptions;
- receiving text and binary frames;
- responding to ping frames;
- sending configured heartbeats;
- reconnecting after failures;
- exponential backoff;
- cancellation.

This part is reusable, but every exchange still has different subscription messages, heartbeat expectations, message formats, and stream semantics.

### `reqwest`

`reqwest` is used for exchange REST APIs.

The shared HTTP client handles:

- request execution;
- query parameters;
- JSON request bodies;
- timeouts;
- response status handling;
- JSON deserialization;
- rate-limiter integration;
- repeated polling.

REST is used for instrument metadata, order-book snapshots, open interest, funding, and other exchange-specific endpoints.

### Serde and Typed Payloads

Serde is used to deserialize exchange JSON into Rust structs.

Instead of moving unstructured JSON through the entire application, each exchange defines native payload types for the messages it actually returns.

This provides:

- explicit expected fields;
- type checking;
- clear optional fields;
- better parsing errors;
- fixture-based tests;
- easier conversion into normalized application events.

It is especially useful because exchange APIs frequently use short field names, inconsistent number representations, and different response wrappers.

### TOML Configuration

TOML files hold connection details and protocol templates such as:

- REST base URLs;
- WebSocket URLs;
- endpoint paths;
- request weights;
- subscription messages;
- heartbeat frames;
- reconnect budgets;
- stream-name templates.

The goal was to avoid hard-coding every URL and message template directly into Rust.

This works for configuration-level differences, but TOML cannot remove semantic differences between exchanges. It can describe a subscription message, but it cannot decide how a sparse ticker update should be interpreted or how an order-book snapshot must be synchronized with deltas.

### Enums and Normalized Events

Rust enums provide the common domain model.

Exchange-specific payloads are converted into normalized `MarketEvent` variants such as:

```text
Trade
DepthDelta
OpenInterest
Funding
Liquidation
```

This is one of the stronger parts of the current design.

Downstream database and Redis code does not need to understand the original Bybit, Binance, or Hyperliquid JSON format. It receives normalized events with consistent fields and scaled integer values.

### Traits

The project uses small traits such as:

- `FromJsonStr`;
- `MapToEvents`;
- database writer interfaces;
- Redis publisher interfaces.

`MapToEvents` separates native exchange payloads from the normalized event model.

That abstraction is worth preserving in a future refactor because exchange parsing will always be exchange-specific, while the rest of the application should operate on shared event types.

### SQLx

SQLx provides asynchronous PostgreSQL access.

It is used for:

- connection pools;
- batch writes;
- stream-registry persistence;
- TimescaleDB storage;
- typed database errors;
- concurrent writer control.

Once an exchange payload has been normalized into a `MarketEvent`, the database layer should generally not care which native API produced it.

### Axum

Axum provides the runtime HTTP API and experimental web interface.

The control API allows streams to be started, stopped, inspected, and tuned without coupling the exchange implementation directly to a CLI or UI.

### Tracing and Prometheus

`tracing` and Prometheus metrics are used because network integrations fail in ways that are difficult to understand from return values alone.

Useful signals include:

- incoming messages;
- processed messages;
- parsing errors;
- reconnect attempts;
- rate-limit waits;
- database write rates;
- database latency;
- dropped rows.

These are particularly important while validating a new exchange.

---

## How Exchange Data Moves Through the System

The intended data path is:

```text
Exchange REST or WebSocket API
            │
            ▼
Exchange-specific Rust payload type
            │
            ▼
Exchange-specific mapping
            │
            ▼
Normalized MarketEvent
            │
            ├── PostgreSQL / TimescaleDB
            ├── optional Redis publishing
            └── Prometheus metrics
```

The important boundary is:

```text
native exchange payload
        ↓
normalized MarketEvent
```

Everything before that boundary is usually exchange-specific.

Everything after that boundary should ideally be shared.

The current project only partially achieves this separation. Mapping is reasonably isolated, but stream-worker functions still combine transport handling, parsing, mapping, batching, Redis publishing, database writing, metrics, and runtime state.

---

## Why There Is No Universal Exchange Adapter

Exchange APIs differ much more than their documentation initially suggests.

Adding another exchange is not simply a matter of changing the WebSocket URL.

### Subscription Formats

Binance can use a string-based stream name such as:

```text
btcusdt@aggTrade
```

Bybit uses a request containing an array of topics.

Hyperliquid uses a nested subscription object containing fields such as subscription type and coin.

### Symbol Conventions

The same market may be represented as:

```text
BTCUSDT
btcusdt
BTC
BTC-USDT
BTC_USDT
```

An exchange may use one representation in REST endpoints and another in WebSocket topics.

### Heartbeats

Exchanges differ in whether they expect:

- WebSocket ping frames;
- JSON ping messages;
- client-side heartbeats;
- only responses to server ping frames;
- no explicit heartbeat at all.

### Order-Book Initialization

A correct order-book integration may require:

- an HTTP snapshot;
- buffering WebSocket deltas during the snapshot;
- update-sequence validation;
- detection of missing updates;
- a full resynchronization after a gap.

The exact procedure is exchange-specific.

### Quantity Units

Reported quantity may represent:

- base-asset units;
- quote-asset value;
- contracts;
- lots;
- coin value multiplied by contract size.

This must be normalized before values from different exchanges can be compared or stored consistently.

### Funding and Open Interest

One exchange may expose funding and open interest separately.

Another may publish both through one ticker or asset-context stream.

That is why the application has a combined:

```text
FundingOpenInterest
```

stream kind.

### Rate Limits

Rate-limit models can be:

- request count;
- weighted requests;
- per-IP;
- per-user;
- per-endpoint;
- header-driven;
- undocumented client-side safety limits.

A generic limiter can provide common mechanics, but exchange-specific interpretation is still necessary.

### Response Shapes

Some APIs return the payload directly.

Others wrap it inside fields such as:

```text
result
data
response
```

Some WebSocket updates contain complete snapshots, while others contain only fields that changed.

For these reasons, there is no reliable copy-and-paste recipe that works for every exchange.

---

## What Is Reusable

Several parts of the current code can be reused when adding another exchange.

### Shared Transport Components

The generic WebSocket client already provides:

- connection management;
- reconnect handling;
- heartbeat support;
- cancellation;
- subscription and unsubscription delivery;
- ingest metrics.

The HTTP client already provides:

- request execution;
- JSON decoding;
- rate-limiter integration;
- polling;
- consistent API errors.

### Shared Domain Model

The normalized event model can represent the currently supported data types.

A new exchange should map into these existing event types whenever possible rather than introducing another exchange-specific representation downstream.

### Shared Outputs

Once events are normalized, the existing output paths can usually be reused:

- DB row conversion;
- TimescaleDB batching;
- Redis field conversion;
- Prometheus counters;
- stream registry;
- runtime knobs.

### Shared Configuration Concepts

The existing exchange TOMLs already provide a common vocabulary for:

- base URLs;
- endpoint definitions;
- request weights;
- WebSocket topics;
- subscription templates;
- reconnect budgets;
- heartbeat configuration.

This reduces hard-coded protocol details, even though it does not eliminate exchange-specific code.

---

## What Is Currently Messy

The main problem is not any single module. It is the number of places that know about every exchange.

### Hard-Coded Exchange Identity

`ExchangeId` explicitly lists every exchange.

Adding another exchange requires updating parsers, display conversions, API deserialization, UI handling, and other match statements.

### Hard-Coded Configuration Loading

The configuration loader explicitly knows the local path, Kubernetes path, and environment-variable name for each exchange.

The exchange configuration container also has one optional field per exchange.

### Hard-Coded Dependency Bootstrap

Application startup explicitly creates:

- a REST client for every exchange;
- a WebSocket client for every exchange;
- limiter entries for every exchange.

### Large Stream-Routing Match

The stream start function branches first by transport, then stream kind, and then exchange.

This makes supported combinations visible, but it also means every new exchange requires adding branches throughout one large routing function.

### Duplicated Stream Workers

Functions such as exchange-specific trade workers perform a very similar sequence:

- resolve the stream;
- open the WebSocket;
- deserialize a payload;
- map it into events;
- convert events into DB rows;
- optionally publish to Redis;
- append to a batch;
- write to the database;
- update runtime state.

The payload type and mapping differ, but much of the surrounding control flow is repeated.

### Cross-Cutting Registration

A new exchange can require changes in:

- exchange enums;
- application toggles;
- configuration loading;
- REST client bootstrap;
- WebSocket client bootstrap;
- limiter registries;
- instrument loading;
- native payload types;
- event mapping;
- stream workers;
- stream routing;
- capabilities;
- API parsers;
- experimental UI parsers;
- SQL setup;
- stream-registry constraints;
- Docker environment paths;
- Kubernetes ConfigMaps and Deployment variables;
- test fixtures.

That is the clearest sign that the current exchange boundary needs refactoring.

---

## Adding an Exchange in the Current Codebase

The following is a map of the work currently required. It is not a guarantee that every exchange will fit exactly into these steps.

### Research the Native API First

Before changing Rust code, determine:

- supported market type;
- instrument naming rules;
- REST base URL;
- WebSocket base URL;
- subscription and unsubscription formats;
- heartbeat behavior;
- reconnect rules;
- rate limits;
- order-book synchronization rules;
- quantity units;
- timestamp units;
- trade-side meaning;
- funding and open-interest behavior.

Save real API responses as fixtures before designing the Rust types.

Exchange documentation alone is often insufficient. Live payloads may contain optional fields, sparse updates, undocumented wrappers, or different numeric representations.

### Add Exchange Identity and Configuration

The new exchange currently needs:

- a new `ExchangeId` variant;
- a toggle in `app.toml`;
- a new exchange TOML;
- a configuration-loader branch;
- a field in `ExchangeConfigs`;
- local and Kubernetes config paths;
- Docker and Deployment environment-variable paths.

The exchange TOML should contain only protocol configuration that can genuinely be represented as data.

Do not force complex exchange behavior into TOML merely to make the system appear generic.

### Define Native Payload Types

Create exchange-specific Rust structs for each required response:

- instrument metadata;
- trades;
- depth snapshots;
- depth updates;
- liquidations;
- funding;
- open interest;
- heartbeat or control responses where necessary.

Use optional fields only where the exchange genuinely sends sparse or optional data.

Store representative payloads under the test-data directory and verify that every payload deserializes before connecting it to the live stream runner.

### Normalize Instrument Metadata

The instrument loader needs to determine:

- canonical symbol;
- market kind;
- base and quote assets;
- contract size;
- reported quantity unit;
- active or delisted status;
- price tick;
- quantity step.

This is essential because payload mapping depends on knowing how native quantities should be interpreted.

### Implement Event Mapping

Implement `MapToEvents` for each native payload type.

The mapper should be responsible for:

- timestamp conversion;
- side normalization;
- symbol normalization;
- fixed-point scaling;
- quantity-unit conversion;
- contract-size conversion;
- extraction of sequence IDs;
- conversion into shared `MarketEvent` variants.

Mapping should not open sockets, write to PostgreSQL, or publish to Redis.

### Register the Clients and Limiters

The new exchange must be added to dependency startup so that its REST and WebSocket clients are created when its application toggle is enabled.

Its rate-limit model must also be added to the HTTP and WebSocket limiter registries.

Do not simply copy another exchange's numerical limits. Incorrect limits can cause either unnecessary throttling or exchange bans.

### Add Stream Workers and Routing

With the current architecture, exchange-specific worker functions must still be added for each supported stream kind.

The workers connect the shared transport client to:

- the correct native payload type;
- the exchange mapper;
- DB row conversion;
- Redis publishing;
- batching;
- cancellation;
- runtime state.

The supported combination must then be added to the main stream-routing function and the capabilities list.

### Update Instruments and API Handling

The new exchange must be recognized by:

- instrument refresh;
- instrument filtering;
- HTTP API path parsing;
- request-body deserialization;
- experimental web UI parsing.

This duplication is a design issue, but it is part of the current implementation.

### Update Database Setup

The SQL setup creates exchange-specific schemas such as:

```text
ex_bybit_linear
```

A new exchange therefore needs its TimescaleDB hypertables created.

The stream registry also has a database constraint listing valid exchange names. That constraint must be updated, or registry inserts for the new exchange will fail.

Schema creation, retention, compression, chunk intervals, and indexes should be reviewed for the expected message volume of the new exchange.

### Update Kubernetes Configuration

The exchange TOML must be added to the application ConfigMap and mounted under:

```text
/etc/mini-fintickstreams/
```

The Deployment also needs the correct versioned configuration-path environment variable.

Generating the ConfigMap directly from `src/config/` is safer than manually maintaining another copy of every TOML file.

---

## Validation Requirements

A new exchange should not be described as supported merely because the project compiles.

### Payload Validation

Every native payload type should parse saved real-world fixtures.

Fixtures should include:

- normal messages;
- sparse updates;
- empty arrays;
- optional fields;
- large values;
- very small prices and quantities;
- snapshots and deltas where applicable.

### Mapping Validation

Mapped events should be checked for:

- correct timestamp;
- correct symbol;
- correct side;
- correct scale;
- correct quantity unit;
- correct sequence number;
- no accidental zeroing of very small values;
- no `i64` overflow for large values.

### Live Transport Validation

A live smoke test should confirm:

- the WebSocket connects;
- the subscription is accepted;
- messages continue arriving;
- heartbeats work;
- cancellation closes the stream;
- reconnect behavior works after interruption.

### Database Validation

Confirm that:

- the correct exchange schema exists;
- rows are being written;
- row counts increase;
- timestamps are current;
- values decode correctly when divided by their scales;
- failed and dropped-row metrics remain at zero.

### Operational Validation

Check the runtime API and monitoring:

```text
stream appears as Running
ingest_processed_total increases
db_rows_written_total increases
ingest_errors_total stays low
ws_reconnect_attempts_total remains stable
DB health remains GREEN
```

Only after the complete path works should the exchange and stream kind be added to the documented support list.

---

## Future Refactor Direction

The current architecture should eventually be reorganized around a real exchange-adapter boundary.

### Exchange Adapter

Each exchange should own one adapter containing:

- identity;
- capabilities;
- configuration;
- symbol normalization;
- instrument loading;
- REST request construction;
- WebSocket subscription construction;
- native decoding;
- mapping into normalized events.

The rest of the application should not need a new match branch every time an exchange is added.

### Generic Stream Runner

The common stream lifecycle should be extracted from exchange-specific workers:

- connect;
- subscribe;
- receive;
- decode;
- map;
- publish;
- batch;
- persist;
- reconnect;
- cancel.

The exchange adapter should provide the parts that differ, while one runner owns the repeated orchestration.

### Separate Layers

A cleaner design would separate:

| Layer | Responsibility |
|---|---|
| Transport | HTTP and WebSocket communication |
| Protocol adapter | Subscription messages, native payloads, heartbeats, rate limits |
| Domain mapper | Native payload into normalized `MarketEvent` |
| Pipeline | Metrics, batching, backpressure, cancellation |
| Sinks | TimescaleDB and optional Redis |
| Control plane | Runtime API, registry, capabilities, knobs |

These concerns currently overlap in several files.

### Registration Instead of Scattered Match Statements

A central adapter registry or factory should replace many of the current exchange-specific matches.

The runtime would request an adapter by `ExchangeId`, and that adapter would expose its capabilities and builders.

### Preserve the Good Parts

A refactor should retain:

- typed native payloads;
- normalized `MarketEvent`;
- fixed-point values;
- fixture-based deserialization tests;
- shared HTTP and WebSocket transport;
- cancellation tokens;
- DB and Redis interfaces;
- Prometheus instrumentation.

The goal is not to rewrite everything. It is to move existing working pieces behind clearer boundaries.

---

## Practical Conclusion

This project demonstrates that Rust can successfully handle a complete concurrent market-data pipeline:

```text
exchange connection
    → typed deserialization
    → normalization
    → runtime control
    → batching
    → TimescaleDB
    → optional Redis
    → Prometheus and Grafana
```

However, the current exchange layer should be understood for what it is:

> A working prototype built while learning Rust, not a finished plug-in architecture.

Bybit Linear is the current working demonstration path.

Adding another exchange is possible, but it currently requires coordinated changes across several modules because the architecture is too fragmented.

I recognize the design problems and would structure the exchange boundary differently today. I do not currently have enough free time to perform the full refactor safely, so the immediate priority is to keep the validated path stable, document the extension points honestly, and avoid presenting unfinished exchange integrations as production-ready.
