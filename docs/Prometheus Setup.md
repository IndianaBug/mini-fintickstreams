
This guide assumes you already know the basics of Prometheus, Linux services, Kubernetes, and PromQL.

## Quick Navigation

- [[#Why Prometheus]]
- [[#Local Setup]]
  - [[#Verify the App Metrics Endpoint]]
  - [[#Install Prometheus]]
  - [[#Add the Scrape Target]]
  - [[#Verify Prometheus]]
- [[#Useful PromQL]]
  - [[#Find Only This Application's Metrics]]
  - [[#Inspect WebSocket Reconnects]]
  - [[#Query Through the HTTP API]]
- [[#Kubernetes Setup]]
  - [[#Use the Existing Kubernetes Templates]]
  - [[#Plain Prometheus Configuration]]
  - [[#Prometheus Operator and ServiceMonitor]]
  - [[#Verify the Kubernetes Target]]
- [[#Troubleshooting]]
- [[#Good Demo Flow]]

---

## Why Prometheus

Prometheus is the observability database for this project.

It does **not** store trades, order-book updates, or other market history. That data belongs in PostgreSQL and TimescaleDB.

Prometheus stores operational metrics such as:

```text
active streams
processed messages
database write rates
write failures
WebSocket reconnects
latency
queue depth
runtime health
```

The application exposes those metrics through an HTTP endpoint. Prometheus periodically calls that endpoint, stores the values with timestamps, and makes them queryable through PromQL. :contentReference[oaicite:0]{index=0}

The monitoring path is:

```text
mini-fintickstreams
        │
        │ /metrics
        ▼
Prometheus
        │
        │ PromQL
        ▼
Grafana / alerts / debugging
```

This is useful because application logs tell you what happened once, while Prometheus lets you inspect behavior over time.

For example:

```text
Did reconnects suddenly increase?
Is the DB writer falling behind?
How many streams are active?
Did ingestion stop five minutes ago?
Is the application still being scraped?
```

The default application metrics endpoint is:

```text
http://localhost:8000/metrics
```

The repository configuration binds the metrics server to port `8000` and exposes the `/metrics` path. :contentReference[oaicite:1]{index=1}

---

## Local Setup

### Verify the App Metrics Endpoint

Start `mini-fintickstreams`, then check the endpoint directly:

```bash
curl -s http://localhost:8000/metrics | head -40
```

You should see Prometheus text-format output:

```text
# HELP ...
# TYPE ...
some_metric_name 1
```

If this endpoint does not work, fix the application metrics server before configuring Prometheus.

---

### Install Prometheus

On Ubuntu or Debian:

```bash
sudo apt update
sudo apt install -y prometheus

sudo systemctl enable --now prometheus
```

Check it:

```bash
systemctl status prometheus
curl http://localhost:9090/-/ready
```

The Prometheus Web UI is available at:

```text
http://localhost:9090
```

The normal package configuration file is:

```text
/etc/prometheus/prometheus.yml
```

---

### Add the Scrape Target

Open the Prometheus configuration:

```bash
sudo nano /etc/prometheus/prometheus.yml
```

Find the existing:

```yaml
scrape_configs:
```

section.

Do **not** add another `scrape_configs:` key. Add a new job under the existing one:

```yaml
scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets:
          - localhost:9090

  - job_name: mini-fintickstreams
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets:
          - localhost:8000
```

The important part is:

```yaml
  - job_name: mini-fintickstreams
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets:
          - localhost:8000
```

Validate the file:

```bash
sudo promtool check config /etc/prometheus/prometheus.yml
```

Then restart Prometheus:

```bash
sudo systemctl restart prometheus
```

---

### Verify Prometheus

Open:

```text
http://localhost:9090/targets
```

The application target should show:

```text
Job:       mini-fintickstreams
Endpoint:  http://localhost:8000/metrics
State:     UP
```

You can also inspect targets through the HTTP API:

```bash
curl -s http://localhost:9090/api/v1/targets | jq
```

A more focused version:

```bash
curl -s http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | {
      job: .labels.job,
      health: .health,
      scrape_url: .scrapeUrl,
      error: .lastError
    }'
```

In the Prometheus query page, run:

```promql
up{job="mini-fintickstreams"}
```

Expected value:

```text
1
```

The values mean:

```text
1 = Prometheus successfully scraped the application
0 = the target exists, but scraping failed
```

---

## Useful PromQL

### Find Only This Application's Metrics

Prometheus adds the configured job name as a label.

Use this filter to separate application metrics from Prometheus itself, Node Exporter, and other targets:

```promql
{job="mini-fintickstreams"}
```

List all metric names currently exposed by this application:

```promql
count by (__name__) (
  {job="mini-fintickstreams"}
)
```

Check only target health:

```promql
up{job="mini-fintickstreams"}
```

Count the number of scraped time series:

```promql
count({job="mini-fintickstreams"})
```

---

### Inspect WebSocket Reconnects

The application exposes:

```promql
ws_reconnect_attempts_total
```

Because the name ends in `_total`, it should be treated as a counter.

Show the current accumulated value:

```promql
ws_reconnect_attempts_total{job="mini-fintickstreams"}
```

Show how many reconnect attempts occurred during the last five minutes:

```promql
increase(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
)
```

Show the average reconnect rate per second:

```promql
rate(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
)
```

A longer window gives a smoother result:

```promql
rate(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[30m]
)
```

Inspect the labels returned by the basic query before grouping.

If the metric includes an `exchange` label, reconnects can be grouped by exchange:

```promql
sum by (exchange) (
  increase(
    ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
  )
)
```

Useful rule of thumb:

```text
Counter
    └── usually query with rate() or increase()

Gauge
    └── usually query directly

Histogram
    └── usually query with histogram_quantile()
```

---

### Query Through the HTTP API

PromQL can also be executed without the Web UI.

Target health:

```bash
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=up{job="mini-fintickstreams"}'
```

WebSocket reconnects:

```bash
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=ws_reconnect_attempts_total{job="mini-fintickstreams"}'
```

Reconnect increase over five minutes:

```bash
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=increase(ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m])'
```

This is useful for scripts, smoke tests, CI checks, and demo automation.

---

## Kubernetes Setup

### Use the Existing Kubernetes Templates

The repository already contains Kubernetes YAML files under:

```text
k8s/
```

These are working templates from the original setup.

They should still be reviewed and adjusted for another cluster, especially:

```text
namespace
Service names
labels
container image
resource limits
storage
Prometheus installation
```

The existing application Service exposes a named `metrics` port on `8000`. :contentReference[oaicite:2]{index=2}

Inside Kubernetes, do not normally configure Prometheus with:

```text
localhost:8000
```

From inside the Prometheus Pod, `localhost` means the Prometheus Pod itself.

Use the Kubernetes Service name instead:

```text
mini-fintickstreams:8000
```

Or, when Prometheus is in another namespace:

```text
mini-fintickstreams.APP_NAMESPACE.svc.cluster.local:8000
```

---

### Plain Prometheus Configuration

If Prometheus uses a normal `prometheus.yml`, add another scrape job:

```yaml
scrape_configs:
  - job_name: mini-fintickstreams
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets:
          - mini-fintickstreams.APP_NAMESPACE.svc.cluster.local:8000
```

Replace:

```text
APP_NAMESPACE
```

with the namespace containing the application.

If Prometheus and the application are in the same namespace, the short Service name is enough:

```yaml
scrape_configs:
  - job_name: mini-fintickstreams
    scrape_interval: 5s
    metrics_path: /metrics
    static_configs:
      - targets:
          - mini-fintickstreams:8000
```

After updating the Prometheus ConfigMap or configuration file, reload or restart Prometheus according to how it was installed.

---

### Prometheus Operator and ServiceMonitor

If the cluster uses Prometheus Operator or `kube-prometheus-stack`, use a `ServiceMonitor`.

First, make sure the application Service has a label that the `ServiceMonitor` can select:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: mini-fintickstreams
  labels:
    app: mini-fintickstreams

spec:
  selector:
    app: mini-fintickstreams

  ports:
    - name: api
      port: 8080
      targetPort: 8080

    - name: metrics
      port: 8000
      targetPort: 8000
```

The `ServiceMonitor` selects the **Service metadata labels**, not the Pod selector.

Example:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: mini-fintickstreams
  namespace: monitoring
  labels:
    release: kube-prometheus-stack

spec:
  namespaceSelector:
    matchNames:
      - APP_NAMESPACE

  selector:
    matchLabels:
      app: mini-fintickstreams

  endpoints:
    - port: metrics
      path: /metrics
      interval: 5s
      scrapeTimeout: 3s
```

Replace:

```text
APP_NAMESPACE
```

with the application namespace.

The label:

```yaml
release: kube-prometheus-stack
```

depends on the Prometheus Operator installation. It may need to match the labels used by existing `ServiceMonitor` resources in the cluster.

Check them with:

```bash
kubectl get servicemonitors \
  --all-namespaces \
  --show-labels
```

Apply the resources:

```bash
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/servicemonitor.yaml
```

---

### Verify the Kubernetes Target

First verify that the metrics endpoint is reachable inside the cluster:

```bash
kubectl run metrics-check \
  --rm \
  -it \
  --restart=Never \
  --image=curlimages/curl \
  -- \
  curl -s http://mini-fintickstreams.APP_NAMESPACE.svc.cluster.local:8000/metrics
```

Check the application Service:

```bash
kubectl -n APP_NAMESPACE get service mini-fintickstreams
kubectl -n APP_NAMESPACE get endpoints mini-fintickstreams
```

If Prometheus is not externally exposed, port-forward its Service:

```bash
kubectl -n monitoring get services
```

Then:

```bash
kubectl -n monitoring port-forward \
  service/PROMETHEUS_SERVICE_NAME \
  9090:9090
```

Open:

```text
http://localhost:9090/targets
```

The application target should be `UP`.

Query:

```promql
up{job=~".*mini-fintickstreams.*"}
```

The exact `job` label may be generated differently when using a `ServiceMonitor`, so inspect the returned labels before finalizing dashboards.

---

## Troubleshooting

### The App Endpoint Works but the Target Is Missing

Check that the scrape job was added under the existing:

```yaml
scrape_configs:
```

section.

Then validate and restart Prometheus:

```bash
sudo promtool check config /etc/prometheus/prometheus.yml
sudo systemctl restart prometheus
```

---

### The Target Is Down Locally

Check the application endpoint:

```bash
curl -v http://localhost:8000/metrics
```

Check whether the port is listening:

```bash
ss -lntp | grep 8000
```

Check the Prometheus target error:

```bash
curl -s http://localhost:9090/api/v1/targets \
  | jq '.data.activeTargets[] | select(.labels.job == "mini-fintickstreams")'
```

---

### The Target Is Down in Kubernetes

Do not use:

```text
localhost:8000
```

Use the application Service DNS name.

Check:

```bash
kubectl -n APP_NAMESPACE get pods
kubectl -n APP_NAMESPACE get service
kubectl -n APP_NAMESPACE get endpoints
```

If using a `ServiceMonitor`, verify:

```text
Service metadata labels
ServiceMonitor selector
namespaceSelector
named metrics port
Prometheus Operator release labels
```

---

### Prometheus Scrapes the App but a Metric Is Missing

Inspect the raw endpoint:

```bash
curl -s http://localhost:8000/metrics \
  | grep ws_reconnect
```

Then ask Prometheus which metric names it currently has:

```promql
count by (__name__) (
  {job="mini-fintickstreams"}
)
```

A metric may not appear until the related code path has been used at least once.

---

## Good Demo Flow

For a short employer-facing demonstration:

```text
1. Open the application's /metrics endpoint
2. Open Prometheus Targets and show the app as UP
3. Run up{job="mini-fintickstreams"}
4. List the application's metric names
5. Query ws_reconnect_attempts_total
6. Use increase(...[5m])
7. Start or stop a stream
8. Show metric values changing over time
9. Open the Graph view and adjust the time range
```

Useful demo queries:

```promql
up{job="mini-fintickstreams"}
```

```promql
count by (__name__) (
  {job="mini-fintickstreams"}
)
```

```promql
ws_reconnect_attempts_total{job="mini-fintickstreams"}
```

```promql
increase(
  ws_reconnect_attempts_total{job="mini-fintickstreams"}[5m]
)
```

That is enough to demonstrate the complete monitoring path:

```text
Rust application
        │
        │ exports metrics
        ▼
Prometheus
        │
        │ stores and queries them
        ▼
PromQL / Grafana / alerts
```