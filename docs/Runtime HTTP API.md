## Quick Navigation

- [[#General Information]]
- [[#Using jq]]
- [[#Health Checks]]
- [[#Stream Management]]
- [[#Stream Knobs]]
- [[#Instruments Registry]]
- [[#Rate Limiters]]
- [[#Experimental Web Dashboard]]
- [[#Error Handling]]

---

## General Information

| Setting | Value |
|---|---|
| Base URL | `http://localhost:8080` |
| Format | JSON |
| Content type | `application/json` |
| Stream identity | `{exchange}/{symbol}/{kind}/{transport}` |
| Experimental dashboard | `http://127.0.0.1:8080/ui` |

A stream is logically identified by:

```text
exchange + symbol + kind + transport
```

For example:

```text
BybitLinear / BTCUSDT / Trades / Ws
```

The API may return the serialized stream ID as:

```text
bybit_linear:BTCUSDT:Ws:Trades
```

JSON request bodies use enum-style names such as:

```text
BybitLinear
BinanceLinear
HyperliquidPerp

Trades
L2Book
FundingOpenInterest

Ws
HttpPoll
```

Paths and query parameters normally use exchange names such as:

```text
bybit_linear
binance_linear
hyperliquid_perp
```

---

## Using `jq`

Most endpoints return JSON. Piping the response through `jq` makes it much easier to read:

```bash
curl -s http://localhost:8080/streams | jq
```

The same pattern works for almost every GET endpoint:

```bash
curl -s http://localhost:8080/health/runtime | jq
curl -s http://localhost:8080/streams/capabilities | jq
curl -s http://localhost:8080/limiters | jq
```

Without `jq`, the response is still valid JSON, but it is usually printed as one long line.

---

## Health Checks

### Runtime Health

```text
GET /health/runtime
```

Checks whether the runtime health guard is currently GREEN.

```bash
curl -s http://localhost:8080/health/runtime | jq
```

Example response:

```json
{
  "ok": true
}
```

A healthy process can still have an unhealthy dependency, so check the database and Redis endpoints separately.

### Database Health

```text
GET /health/db
```

Returns `ok: true` when the database integration is enabled, initialized, reachable, and currently able to admit new streams.

```bash
curl -s http://localhost:8080/health/db | jq
```

### Redis Health

```text
GET /health/redis
```

Returns `ok: true` when Redis is enabled, initialized, and currently able to publish.

```bash
curl -s http://localhost:8080/health/redis | jq
```

Check all health endpoints together:

```bash
for endpoint in runtime db redis; do
  echo "=== $endpoint ==="
  curl -s "http://localhost:8080/health/$endpoint" | jq
done
```

---

## Stream Management

### List Capabilities

```text
GET /streams/capabilities
```

Returns the supported exchange, transport, and stream-kind combinations.

```bash
curl -s http://localhost:8080/streams/capabilities | jq
```

Capabilities for a single exchange may also be available through:

```text
GET /streams/capabilities/{exchange}
```

Example:

```bash
curl -s \
  http://localhost:8080/streams/capabilities/bybit_linear \
  | jq
```

### List Active Streams

```text
GET /streams
```

Returns all streams currently registered in runtime memory.

```bash
curl -s http://localhost:8080/streams | jq
```

Supported query filters:

```text
exchange
symbol
kind
transport
```

Example:

```bash
curl -s \
  "http://localhost:8080/streams?exchange=bybit_linear&symbol=BTCUSDT" \
  | jq
```

Example response:

```json
[
  {
    "id": "bybit_linear:BTCUSDT:Ws:Trades",
    "status": "Running",
    "exchange": "bybit_linear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }
]
```

### Get Stream Count

```text
GET /streams/count
```

```bash
curl -s http://localhost:8080/streams/count | jq
```

Example response:

```json
{
  "count": 4
}
```

### Get a Specific Stream

```text
GET /streams/{exchange}/{symbol}/{kind}/{transport}
```

Example:

```bash
curl -s \
  http://localhost:8080/streams/bybit_linear/BTCUSDT/Trades/Ws \
  | jq
```

Example response:

```json
{
  "id": "bybit_linear:BTCUSDT:Ws:Trades",
  "status": "Running",
  "spec": {
    "exchange": "bybit_linear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }
}
```

### Start a Stream

```text
POST /streams
```

The request body contains:

```json
{
  "exchange": "BybitLinear",
  "symbol": "BTCUSDT",
  "kind": "Trades",
  "transport": "Ws"
}
```

Full example:

```bash
curl -s -X POST http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "BybitLinear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }' \
  | jq
```

Successful response:

```json
"ok"
```

Starting the same stream twice returns a conflict:

```json
{
  "error": "bybit_linear:BTCUSDT:Ws:Trades",
  "kind": "stream_exists"
}
```

### Stop a Stream

```text
DELETE /streams
```

Stopping a stream uses the same JSON body as starting it:

```bash
curl -s -X DELETE http://localhost:8080/streams \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "BybitLinear",
    "symbol": "BTCUSDT",
    "kind": "Trades",
    "transport": "Ws"
  }' \
  | jq
```

Successful response:

```json
"ok"
```

Verify that it was removed:

```bash
curl -s http://localhost:8080/streams | jq
```

---

## Stream Knobs

Stream knobs allow selected runtime behavior to be changed without restarting the stream.

### View Current Knobs

```text
GET /streams/{exchange}/{symbol}/{kind}/{transport}/knobs
```

Example:

```bash
curl -s \
  http://localhost:8080/streams/bybit_linear/BTCUSDT/Trades/Ws/knobs \
  | jq
```

Example response:

```json
{
  "knobs": {
    "disable_db_writes": false,
    "disable_redis_publishes": false,
    "flush_rows": 1000,
    "flush_interval_ms": 50,
    "chunk_rows": 5000,
    "hard_cap_rows": 10000
  }
}
```

The main settings are:

| Knob | Meaning |
|---|---|
| `disable_db_writes` | Stops database writes while leaving the stream running |
| `disable_redis_publishes` | Stops Redis publishing while leaving the stream running |
| `flush_rows` | Row threshold that can trigger a database flush |
| `flush_interval_ms` | Maximum time buffered rows should wait before becoming flushable |
| `chunk_rows` | Maximum rows used for one database insert chunk |
| `hard_cap_rows` | Safety limit for buffered rows |

### Update Knobs

```text
PATCH /streams/{exchange}/{symbol}/{kind}/{transport}/knobs
```

PATCH semantics are used, so only the supplied fields are changed.

Example:

```bash
curl -s -X PATCH \
  http://localhost:8080/streams/bybit_linear/BTCUSDT/Trades/Ws/knobs \
  -H "Content-Type: application/json" \
  -d '{
    "flush_rows": 500,
    "flush_interval_ms": 100
  }' \
  | jq
```

Another example:

```bash
curl -s -X PATCH \
  http://localhost:8080/streams/bybit_linear/BTCUSDT/Trades/Ws/knobs \
  -H "Content-Type: application/json" \
  -d '{
    "disable_db_writes": true
  }' \
  | jq
```

Knob changes are applied live. Be careful with output-disabling switches because the stream may continue running while no longer publishing or persisting data.

---

## Instruments Registry

The instruments registry contains the exchange instruments discovered by the application.

### List Instruments

```text
GET /instruments
```

At least one filter is normally required:

```text
exchange
kind
```

Example:

```bash
curl -s \
  "http://localhost:8080/instruments?exchange=bybit_linear" \
  | jq
```

### Count Instruments

```text
GET /instruments/count
```

Example:

```bash
curl -s \
  "http://localhost:8080/instruments/count?exchange=bybit_linear" \
  | jq
```

### Check Whether an Instrument Exists

```text
GET /instruments/exists
```

Required query parameters:

```text
exchange
symbol
```

Example:

```bash
curl -s \
  "http://localhost:8080/instruments/exists?exchange=bybit_linear&symbol=BTCUSDT" \
  | jq
```

Example response:

```json
{
  "exists": true
}
```

### Refresh the Registry

```text
POST /instruments/refresh
```

Reloads instrument metadata from the configured exchange sources.

```bash
curl -s -X POST \
  http://localhost:8080/instruments/refresh \
  | jq
```

---

## Rate Limiters

### Get All Limiter Budgets

```text
GET /limiters
```

```bash
curl -s http://localhost:8080/limiters | jq
```

### Filter by Exchange

```text
GET /limiters?exchange={exchange}
```

Examples:

```bash
curl -s \
  "http://localhost:8080/limiters?exchange=bybit_linear" \
  | jq

curl -s \
  "http://localhost:8080/limiters?exchange=hyperliquid_perp" \
  | jq
```

Each exchange may return:

| Field | Meaning |
|---|---|
| `http_remaining` | Remaining HTTP request weight, when applicable |
| `ws_subscribe_remaining` | Remaining WebSocket subscription attempts |
| `ws_reconnect_remaining` | Remaining WebSocket reconnect attempts |

These values are useful when starting many streams or diagnosing why admission is temporarily delayed or rejected.

---

## Experimental Web Dashboard

A small built-in dashboard is available at:

[http://127.0.0.1:8080/ui](http://127.0.0.1:8080/ui)

It currently provides basic access to:

```text
health status
active streams
stream start and stop controls
live stream knobs
capabilities
rate-limiter budgets
instrument search
```

The dashboard is **experimental and only minimally functional**. It contains known bugs and should currently be treated as a convenience layer for demonstrations and quick checks.

For reliable automation, debugging, and integrations, use the JSON API directly.

```text
Built-in dashboard
    → convenient manual control

HTTP API
    → authoritative control interface
```

---

## Error Handling

Errors use a consistent JSON structure:

```json
{
  "error": "human-readable message",
  "kind": "machine_readable_error_code"
}
```

Common status codes:

| Status | Meaning |
|---|---|
| `400` | Invalid request or unsupported argument |
| `404` | Stream or resource not found |
| `409` | Resource already exists or conflicts with current state |
| `503` | Runtime or dependency health prevents the operation |
| `500` | Unexpected internal error |

Common error kinds include:

```text
stream_exists
stream_not_found
invalid_argument
disabled
```

When scripting against the API, check both the HTTP status code and the JSON `kind`.

---

## Notes

- Stream knob updates are live and non-blocking.
- Combined streams such as `FundingOpenInterest` are preferred where supported.
- Health endpoints reflect the service's admission and dependency state.
- A running process is not automatically a healthy process; check runtime, database, and Redis separately.
- The built-in dashboard is experimental. The HTTP API remains the main supported control surface.