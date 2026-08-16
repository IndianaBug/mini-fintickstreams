
This guide assumes you already know the basics of Grafana, Prometheus, Linux services, Kubernetes, and PromQL.

## Quick Navigation

- [Why Grafana](#why-grafana)
- [Dashboard Files](#dashboard-files)
- [Local Setup](#local-setup)
  - [Install Grafana](#install-grafana)
  - [Check Prometheus First](#check-prometheus-first)
  - [Provision the Dashboard](#provision-the-dashboard)
  - [Open the Dashboard](#open-the-dashboard)
  - [Update the Dashboard](#update-the-dashboard)
- [What the Dashboard Shows](#what-the-dashboard-shows)
- [Kubernetes Setup](#kubernetes-setup)
  - [Kubernetes Networking](#kubernetes-networking)
  - [Create the Kubernetes Datasource File](#create-the-kubernetes-datasource-file)
  - [Create the ConfigMaps](#create-the-configmaps)
  - [Mount the Files into Grafana](#mount-the-files-into-grafana)
  - [Reload Grafana in Kubernetes](#reload-grafana-in-kubernetes)
- [Troubleshooting](#troubleshooting)
---

## Why Grafana

Monitoring matters because a streaming service can still be **running while already failing operationally**. Unstable network connections can trigger WebSocket reconnects, server load can increase processing lag, PostgreSQL can become saturated by writers, and latency can exceed what a trading strategy can tolerate.

Grafana provides an immediate view of those conditions so you can quickly see **whether data is flowing, where pressure is building, whether latency is increasing, and whether any data is being dropped**.

Grafana sits on top of Prometheus and turns the PromQL results into dashboards, graphs, status panels, and alerts.

```text
mini-fintickstreams
        │
        │ /metrics
        ▼
Prometheus
        │
        │ PromQL
        ▼
Grafana
        │
        ├── status panels
        ├── throughput graphs
        ├── latency graphs
        └── failure indicators
```

Grafana does not replace Prometheus and does not query the Rust application directly.

The connection is:

```text
Grafana → Prometheus → application metrics
```

---

## Dashboard Files

The repository contains three Grafana files:

```text
mini-fintickstreams/
└── grafana/
    ├── prometheus.yaml
    ├── dashboards.yaml
    └── mini-fintickstreams.json
```

Their responsibilities are:

```text
prometheus.yaml
    └── creates the Prometheus datasource

dashboards.yaml
    └── tells Grafana where dashboard JSON files are stored

mini-fintickstreams.json
    └── defines the dashboard, panels, queries, layout, and thresholds
```

These files make the dashboard reproducible. A new Grafana installation can load the same dashboard without manually recreating every panel.

Treat:

```text
mini-fintickstreams.json
```

as the source of truth. UI edits may be overwritten the next time the provisioned JSON is copied or updated.

---

## Local Setup

### Install Grafana

On Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https wget gnupg

sudo mkdir -p /etc/apt/keyrings

sudo wget -O /etc/apt/keyrings/grafana.asc \
  https://apt.grafana.com/gpg-full.key

sudo chmod 644 /etc/apt/keyrings/grafana.asc

echo "deb [signed-by=/etc/apt/keyrings/grafana.asc] https://apt.grafana.com stable main" \
  | sudo tee /etc/apt/sources.list.d/grafana.list

sudo apt-get update
sudo apt-get install -y grafana

sudo systemctl enable --now grafana-server
```

Check it:

```bash
systemctl status grafana-server --no-pager
```

Grafana is normally available at:

```text
http://localhost:3000
```

---

### Check Prometheus First

Grafana depends on Prometheus for this dashboard.

Confirm Prometheus is ready:

```bash
curl http://localhost:9090/-/ready
```

Confirm Prometheus is scraping the application:

```bash
curl -s http://localhost:9090/api/v1/query \
  --get \
  --data-urlencode 'query=up{job="mini-fintickstreams"}'
```

The result should contain:

```text
1
```

See [[Prometheus Setup]] for the full Prometheus configuration.

---

### Provision the Dashboard

The commands below do **not** assume that your terminal is currently inside the `grafana/` directory.

First, set the absolute path to your cloned project:

```bash
export PROJECT_ROOT="/absolute/path/to/mini-fintickstreams"
export GRAFANA_FILES="$PROJECT_ROOT/grafana"
```

Example:

```bash
export PROJECT_ROOT="/home/your-user/projects/mini-fintickstreams"
export GRAFANA_FILES="$PROJECT_ROOT/grafana"
```

Verify that all three files exist:

```bash
ls -l \
  "$GRAFANA_FILES/prometheus.yaml" \
  "$GRAFANA_FILES/dashboards.yaml" \
  "$GRAFANA_FILES/mini-fintickstreams.json"
```

Create the dashboard directory:

```bash
sudo install -d \
  -o grafana \
  -g grafana \
  /var/lib/grafana/dashboards/mini-fintickstreams
```

Install the Prometheus datasource definition:

```bash
sudo install \
  -m 0644 \
  "$GRAFANA_FILES/prometheus.yaml" \
  /etc/grafana/provisioning/datasources/mini-fintickstreams-prometheus.yaml
```

Install the dashboard provider:

```bash
sudo install \
  -m 0644 \
  "$GRAFANA_FILES/dashboards.yaml" \
  /etc/grafana/provisioning/dashboards/mini-fintickstreams.yaml
```

Install the dashboard JSON:

```bash
sudo install \
  -o grafana \
  -g grafana \
  -m 0644 \
  "$GRAFANA_FILES/mini-fintickstreams.json" \
  /var/lib/grafana/dashboards/mini-fintickstreams/mini-fintickstreams.json
```

Restart Grafana:

```bash
sudo systemctl restart grafana-server
```

Check that it started cleanly:

```bash
systemctl status grafana-server --no-pager
```

---

### Open the Dashboard

Open:

```text
http://localhost:3000
```

The provisioned dashboard should appear under:

```text
Dashboards
└── mini-fintickstreams
    └── mini-fintickstreams — Runtime Overview
```

No manual panel creation is required.

---

### Update the Dashboard

After changing:

```text
grafana/mini-fintickstreams.json
```

copy the updated file again:

```bash
export PROJECT_ROOT="/absolute/path/to/mini-fintickstreams"
export GRAFANA_FILES="$PROJECT_ROOT/grafana"

sudo install \
  -o grafana \
  -g grafana \
  -m 0644 \
  "$GRAFANA_FILES/mini-fintickstreams.json" \
  /var/lib/grafana/dashboards/mini-fintickstreams/mini-fintickstreams.json
```

Grafana can detect dashboard-file changes through the configured provider.

Restarting is still the simplest deterministic reload:

```bash
sudo systemctl restart grafana-server
```

If only the dashboard JSON changed, a full Grafana reinstall is not needed.

If either provisioning YAML file changed, reinstall that file as well:

```bash
sudo install \
  -m 0644 \
  "$GRAFANA_FILES/prometheus.yaml" \
  /etc/grafana/provisioning/datasources/mini-fintickstreams-prometheus.yaml

sudo install \
  -m 0644 \
  "$GRAFANA_FILES/dashboards.yaml" \
  /etc/grafana/provisioning/dashboards/mini-fintickstreams.yaml

sudo systemctl restart grafana-server
```

---

## What the Dashboard Shows

The dashboard focuses on the most useful operational metrics.

### System Health

```text
app_health
runtime_health
streams_active
db_health_state
redis_enabled_state
```

These provide a quick view of whether the service and its main dependencies are usable.

### Ingestion

```text
ingest_processed_total
ingest_errors_total
ingest_lag_seconds
ws_reconnect_attempts_total
```

These show whether market data is being processed, whether the pipeline is falling behind, and whether WebSocket connections are unstable.

### Database Performance

```text
db_rows_written_total
db_write_latency_seconds
db_writer_queue_depth
```

These confirm that data reaches PostgreSQL/TimescaleDB and reveal write-path pressure.

### Data Safety

```text
db_failed_batches_total
db_rows_dropped_total
```

These should normally remain at zero.

An increase in:

```text
db_rows_dropped_total
```

means market data was discarded and should be investigated.

---

## Kubernetes Setup

The same provisioning files can be used in Kubernetes, but the networking and file delivery are different.

The examples below are templates. Adjust:

```text
namespace
Grafana Deployment name
Grafana container name
Prometheus Service name
Prometheus namespace
```

for the actual cluster.

---

### Kubernetes Networking

This local datasource URL:

```text
http://localhost:9090
```

normally does not work from a Grafana Pod.

Inside the Grafana Pod, `localhost` means:

```text
the Grafana Pod itself
```

Grafana must connect to the Kubernetes Service exposing Prometheus.

The address normally looks like:

```text
http://PROMETHEUS_SERVICE.PROMETHEUS_NAMESPACE.svc.cluster.local:9090
```

For example:

```text
http://prometheus.monitoring.svc.cluster.local:9090
```

Check the real Service name:

```bash
kubectl get services --all-namespaces | grep -i prometheus
```

---

### Create the Kubernetes Datasource File

Keep the local file:

```text
grafana/prometheus.yaml
```

for local Linux.

Create a separate Kubernetes version:

```text
grafana/prometheus-k8s.yaml
```

Example:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    uid: mini-fintickstreams-prometheus
    type: prometheus
    access: proxy
    url: http://PROMETHEUS_SERVICE.PROMETHEUS_NAMESPACE.svc.cluster.local:9090
    isDefault: true
    editable: true
```

Replace:

```text
PROMETHEUS_SERVICE
PROMETHEUS_NAMESPACE
```

with the values used by the cluster.

If another Grafana datasource is already configured as the default, change:

```yaml
isDefault: true
```

to:

```yaml
isDefault: false
```

---

### Create the ConfigMaps

Set the project path and Grafana namespace:

```bash
export PROJECT_ROOT="/absolute/path/to/mini-fintickstreams"
export GRAFANA_FILES="$PROJECT_ROOT/grafana"
export GRAFANA_NAMESPACE="monitoring"
```

Create or update the datasource ConfigMap:

```bash
kubectl -n "$GRAFANA_NAMESPACE" create configmap \
  mini-fintickstreams-grafana-datasource \
  --from-file=prometheus.yaml="$GRAFANA_FILES/prometheus-k8s.yaml" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Create or update the dashboard-provider ConfigMap:

```bash
kubectl -n "$GRAFANA_NAMESPACE" create configmap \
  mini-fintickstreams-grafana-provider \
  --from-file=dashboards.yaml="$GRAFANA_FILES/dashboards.yaml" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Create or update the dashboard ConfigMap:

```bash
kubectl -n "$GRAFANA_NAMESPACE" create configmap \
  mini-fintickstreams-grafana-dashboard \
  --from-file=mini-fintickstreams.json="$GRAFANA_FILES/mini-fintickstreams.json" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Check them:

```bash
kubectl -n "$GRAFANA_NAMESPACE" get configmaps \
  | grep mini-fintickstreams-grafana
```

---

### Mount the Files into Grafana

Add the following mounts to the Grafana container in its Deployment.

```yaml
spec:
  template:
    spec:
      containers:
        - name: grafana
          volumeMounts:
            - name: mini-fintickstreams-datasource
              mountPath: /etc/grafana/provisioning/datasources/mini-fintickstreams-prometheus.yaml
              subPath: prometheus.yaml
              readOnly: true

            - name: mini-fintickstreams-provider
              mountPath: /etc/grafana/provisioning/dashboards/mini-fintickstreams.yaml
              subPath: dashboards.yaml
              readOnly: true

            - name: mini-fintickstreams-dashboard
              mountPath: /var/lib/grafana/dashboards/mini-fintickstreams/mini-fintickstreams.json
              subPath: mini-fintickstreams.json
              readOnly: true

      volumes:
        - name: mini-fintickstreams-datasource
          configMap:
            name: mini-fintickstreams-grafana-datasource

        - name: mini-fintickstreams-provider
          configMap:
            name: mini-fintickstreams-grafana-provider

        - name: mini-fintickstreams-dashboard
          configMap:
            name: mini-fintickstreams-grafana-dashboard
```

The path used by `dashboards.yaml` must match the dashboard mount:

```text
/var/lib/grafana/dashboards/mini-fintickstreams
```

If Grafana was installed using Helm, add the equivalent ConfigMap volumes and mounts through the chart's values rather than editing generated resources permanently.

---

### Reload Grafana in Kubernetes

Because the example uses `subPath` mounts, restart the Grafana Pod after changing any ConfigMap.

Set the actual Deployment name:

```bash
export GRAFANA_NAMESPACE="monitoring"
export GRAFANA_DEPLOYMENT="grafana"
```

Restart it:

```bash
kubectl -n "$GRAFANA_NAMESPACE" rollout restart \
  deployment/"$GRAFANA_DEPLOYMENT"
```

Wait for it:

```bash
kubectl -n "$GRAFANA_NAMESPACE" rollout status \
  deployment/"$GRAFANA_DEPLOYMENT"
```

Check the logs:

```bash
kubectl -n "$GRAFANA_NAMESPACE" logs \
  deployment/"$GRAFANA_DEPLOYMENT" \
  --tail=200
```

Access Grafana locally:

```bash
kubectl -n "$GRAFANA_NAMESPACE" port-forward \
  deployment/"$GRAFANA_DEPLOYMENT" \
  3000:3000
```

Then open:

```text
http://localhost:3000
```

The Kubernetes dashboard path is:

```text
ConfigMaps
    │
    ├── datasource YAML
    ├── dashboard-provider YAML
    └── dashboard JSON
            │
            ▼
       Grafana Pod
            │
            ▼
      Prometheus Service
            │
            ▼
mini-fintickstreams metrics
```

---

## Troubleshooting

### Dashboard Does Not Appear Locally

Check Grafana logs:

```bash
sudo journalctl \
  -u grafana-server \
  -n 200 \
  --no-pager
```

Confirm the files were installed:

```bash
sudo ls -l \
  /etc/grafana/provisioning/datasources/mini-fintickstreams-prometheus.yaml \
  /etc/grafana/provisioning/dashboards/mini-fintickstreams.yaml \
  /var/lib/grafana/dashboards/mini-fintickstreams/mini-fintickstreams.json
```

Restart Grafana:

```bash
sudo systemctl restart grafana-server
```

---

### Datasource Exists but Cannot Reach Prometheus

Locally:

```bash
curl http://localhost:9090/-/ready
```

In Kubernetes, test Prometheus from the Grafana Pod:

```bash
kubectl -n "$GRAFANA_NAMESPACE" exec \
  deployment/"$GRAFANA_DEPLOYMENT" \
  -- wget -qO- \
  http://PROMETHEUS_SERVICE.PROMETHEUS_NAMESPACE.svc.cluster.local:9090/-/ready
```

Use the real Prometheus Service and namespace.

---

### Panels Show `No data`

Test the same query in Prometheus:

```promql
up{job="mini-fintickstreams"}
```

Confirm the application metrics endpoint works:

```bash
curl -s http://localhost:8000/metrics | head -40
```

Confirm Prometheus has application metrics:

```promql
count by (__name__) (
  {job="mini-fintickstreams"}
)
```

A panel may also show no data until the related operation has happened at least once.

For example:

```text
ws_reconnect_attempts_total
```

may remain zero while the connection is stable.

---

### Kubernetes Dashboard Does Not Update

Recreate the ConfigMap:

```bash
kubectl -n "$GRAFANA_NAMESPACE" create configmap \
  mini-fintickstreams-grafana-dashboard \
  --from-file=mini-fintickstreams.json="$GRAFANA_FILES/mini-fintickstreams.json" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Then restart Grafana:

```bash
kubectl -n "$GRAFANA_NAMESPACE" rollout restart \
  deployment/"$GRAFANA_DEPLOYMENT"
```

The provisioned JSON remains the source of truth for the dashboard.
