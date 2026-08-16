
This guide assumes you already know the basics of Redis, Linux services, Kubernetes, and environment variables.

## Quick Navigation

- [[#Why Redis Was Added]]
- [[#Why Redis Is Not the Production Hot Path]]
- [[#How This Project Uses Redis]]
- [[#Local Setup]]
- [[#Kubernetes Setup]]
- [[#Monitoring Redis]]
- [[#Disabling Redis]]

---

## Why Redis Was Added

Redis was originally added as a real-time delivery layer between `mini-fintickstreams` and downstream trading bots.

The idea was straightforward:

```text
Exchange WebSocket
        │
        ▼
mini-fintickstreams
        │
        ├── PostgreSQL / TimescaleDB
        │       └── durable history
        │
        └── Redis Streams
                └── live events for bots and feature builders
```

Redis Streams looked attractive because they provide ordered messages, short-term retention, replay, and consumer groups without requiring every trading bot to connect directly to every exchange.

In this project, Redis is **producer-only**. `mini-fintickstreams` publishes normalized market events, while external bots or feature builders are expected to consume them separately.

The current stream keys use:

```text
stream:{exchange}:{symbol}:{kind}
```

For example:

```text
stream:bybit_linear:BTCUSDT:trades
```

Trades, depth updates, liquidations, funding, and open interest can all be published. Streams use approximate `MAXLEN` retention and currently keep roughly the latest `5,000` entries per key.

---

## Why Redis Is Not the Production Hot Path

Redis itself is fast and can absolutely be used in production. The problem is using it as a **mandatory per-event hop for latency-sensitive trading bots**.

Every event must be:

```text
serialized
    ↓
sent over TCP
    ↓
written with XADD
    ↓
read by another process
    ↓
deserialized again
```

Redis may execute the command very quickly, but network round trips, kernel scheduling, serialization, and system jitter can dominate the actual end-to-end latency.

Kubernetes makes this less predictable because Redis normally runs in another Pod and is reached through a Kubernetes Service. This introduces additional networking and potentially cross-node traffic.

For bots that tolerate a few milliseconds, Redis can still be useful for:

- feature generation
- non-critical strategies
- dashboards and monitoring consumers
- temporary replay
- decoupled data processing

For bots with strict p95 or p99 latency requirements, Redis Streams are probably not the right main live-data path.

In-process channels, shared memory, Unix sockets, or tightly colocated services are better suited when the goal is to minimize latency as much as possible.

The current code therefore treats Redis as **optional acceleration**, not as the source of truth.

If Redis becomes unavailable or too slow, publishing can be skipped or disabled while the TimescaleDB write path continues.

The default Redis health configuration also monitors latency, memory pressure, and pending messages. Redis can be temporarily disabled when those limits are exceeded.

---

## How This Project Uses Redis

Redis is controlled in two places.

The application-level switch is in:

```text
src/config/app.toml
```

and must contain:

```toml
[redis]
enabled = true
```

This determines whether the Redis client and manager are created when the application starts.

The Redis-specific settings are in:

```text
src/config/redis.toml
```

The current setup is roughly:

```text
mode:                 single node
address:              redis://127.0.0.1:6379
environment override: REDIS_NODE_A
stream retention:     approximately 5,000 entries
stream key format:    stream:{exchange}:{symbol}:{kind}
failure policy:       disable Redis temporarily
database behavior:    continue writing
```

The consumer-group name:

```text
cg:features
```

is intended for downstream consumers.

`mini-fintickstreams` itself publishes to Redis Streams but does not consume those streams.

---

## Local Setup

Install Redis on Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y redis-server

sudo systemctl enable --now redis-server

redis-cli ping
redis-server --version
```

A healthy Redis instance should respond:

```text
PONG
```

The default local address is:

```text
redis://127.0.0.1:6379
```

When running the application using environment-based configuration, set:

```bash
export REDIS_NODE_A="redis://127.0.0.1:6379"
```

When using the project's file-based configuration, this is normally unnecessary because `src/config/redis.toml` already contains the local Redis address.

Start `mini-fintickstreams` normally and verify Redis health:

```bash
curl http://localhost:8080/health/redis
```

Expected response:

```json
{"ok":true}
```

You can inspect the Redis Streams created by the application:

```bash
redis-cli --scan --pattern 'stream:*'

redis-cli XLEN stream:bybit_linear:BTCUSDT:trades

redis-cli XREVRANGE \
  stream:bybit_linear:BTCUSDT:trades \
  + - COUNT 3
```

The local architecture is:

```text
Exchange
    │
    ▼
mini-fintickstreams
    │
    ├──────────────► TimescaleDB
    │                  durable history
    │
    └──────────────► Redis :6379
                       temporary live stream
```

Redis should normally remain accessible only from the local machine or a private network.

Do not expose port `6379` directly to the public internet.

---

## Kubernetes Setup

The repository already contains working Kubernetes templates under:

```text
k8s/
```

They are templates from the original deployment and will normally need adjustments for:

```text
namespace
container image
resources
credentials
Service names
storage
```

The Kubernetes application configuration expects Redis to be reachable through a Kubernetes Service.

Instead of:

```text
redis://127.0.0.1:6379
```

the application can use:

```text
redis://redis:6379
```

where:

```text
redis
```

is the Kubernetes Service name.

Create a Redis Deployment and Service, for example:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis

spec:
  selector:
    app: redis

  ports:
    - name: redis
      port: 6379
      targetPort: redis

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis

spec:
  replicas: 1

  selector:
    matchLabels:
      app: redis

  template:
    metadata:
      labels:
        app: redis

    spec:
      containers:
        - name: redis
          image: redis:7-alpine

          args:
            - redis-server
            - --save
            - ""
            - --appendonly
            - "no"
            - --maxmemory
            - "512mb"
            - --maxmemory-policy
            - noeviction

          ports:
            - name: redis
              containerPort: 6379

          readinessProbe:
            exec:
              command:
                - redis-cli
                - ping
            initialDelaySeconds: 2
            periodSeconds: 5

          livenessProbe:
            exec:
              command:
                - redis-cli
                - ping
            initialDelaySeconds: 10
            periodSeconds: 10

          resources:
            requests:
              cpu: "100m"
              memory: "128Mi"
            limits:
              cpu: "1"
              memory: "768Mi"
```

This configuration deliberately treats Redis as an **ephemeral live-data buffer**.

Redis persistence is disabled because TimescaleDB remains the durable source of truth:

```text
Redis
    └── temporary live data

TimescaleDB
    └── durable historical data
```

The application environment should contain:

```text
REDIS_NODE_A=redis://redis:6379
```

For example, in the project's Kubernetes Secret:

```yaml
REDIS_NODE_A: "redis://redis:6379"
```

Keep any other application secrets already required by the project and do not commit real credentials.

Apply the Kubernetes resources:

```bash
export NS="mini-fintickstreams"

kubectl create namespace "$NS" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -

kubectl -n "$NS" apply -f k8s/redis.yaml
kubectl -n "$NS" apply -f k8s/configmap.yaml
kubectl -n "$NS" apply -f k8s/secret.yaml
kubectl -n "$NS" apply -f k8s/deployment.yaml
kubectl -n "$NS" apply -f k8s/service.yaml
```

Check the deployments:

```bash
kubectl -n "$NS" rollout status deployment/redis

kubectl -n "$NS" rollout status \
  deployment/mini-fintickstreams
```

Test Redis from inside the cluster:

```bash
kubectl -n "$NS" run redis-check \
  --rm \
  -it \
  --restart=Never \
  --image=redis:7-alpine \
  -- redis-cli -h redis ping
```

Expected:

```text
PONG
```

Pods in the same namespace can reach Redis using:

```text
redis:6379
```

From another namespace, use the full Kubernetes DNS name:

```text
redis.NAMESPACE.svc.cluster.local:6379
```

The resulting architecture is:

```text
Exchange
    │
    ▼
mini-fintickstreams Pod
    │
    ├──────────────► TimescaleDB Service
    │
    └──────────────► Redis Service
                          │
                          ▼
                       Redis Pod
```

This setup is fine for development, demonstrations, home clusters, feature pipelines, and non-critical consumers.

It should **not** be treated as a deterministic low-latency transport architecture for latency-sensitive trading bots.

---

## Monitoring Redis

The application exposes Redis metrics through the normal Prometheus endpoint.

Important metrics include:

```text
redis_enabled_state
redis_stream_published_total
redis_publish_latency_seconds
redis_publish_failures_total
redis_publish_queue_depth
redis_disable_events_total
```

The most useful Grafana queries are Redis availability:

```promql
redis_enabled_state{job="mini-fintickstreams"}
```

publish latency:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(
      redis_publish_latency_seconds_bucket{
        job="mini-fintickstreams"
      }[5m]
    )
  )
)
```

and recent publish failures:

```promql
increase(
  redis_publish_failures_total{
    job="mini-fintickstreams"
  }[5m]
)
```

These are particularly useful if Redis is being evaluated as a live-data transport because they show the exact problem we care about:

```text
Is Redis available?
Is latency increasing?
Is the publish queue growing?
Are messages failing to publish?
```

---

## Disabling Redis

Redis is optional.

To remove it from the application path entirely, change:

```toml
[redis]
enabled = false
```

and restart the application.

The resulting architecture becomes:

```text
Exchange
    │
    ▼
mini-fintickstreams
    │
    ▼
PostgreSQL / TimescaleDB
```

Database persistence continues normally while Redis publishing is disabled.

This is a perfectly valid configuration when Redis is unnecessary or when its latency characteristics are not suitable for the downstream trading system.