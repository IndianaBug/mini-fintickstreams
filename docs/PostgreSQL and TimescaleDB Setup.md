
This guide assumes you already know the basics of PostgreSQL, `psql`, Kubernetes, and environment variables.

## Quick Navigation

- [Why This Project Uses PostgreSQL and TimescaleDB](#why-this-project-uses-postgresql-and-timescaledb)
- [Is It Enough for Large Amounts of Market Data?](#is-it-enough-for-large-amounts-of-market-data)
- [Where It Starts to Struggle](#where-it-starts-to-struggle)
- [Retention, Compression, and Chunk Settings](#retention-compression-and-chunk-settings)
- [Local Setup](#local-setup)
  - [Install PostgreSQL and TimescaleDB](#install-postgresql-and-timescaledb)
  - [Create Your User and Database](#create-your-user-and-database)
  - [Run the Database Setup Files](#run-the-database-setup-files)
  - [Set SHARD_MAIN_DSN](#set-shard_main_dsn)
- [Kubernetes Setup](#kubernetes-setup)
  - [Kubernetes Networking Is Different](#kubernetes-networking-is-different)
  - [Create Database Credentials](#create-database-credentials)
  - [Example TimescaleDB StatefulSet](#example-timescaledb-statefulset)
  - [Initialize the Kubernetes Database](#initialize-the-kubernetes-database)
  - [Set SHARD_MAIN_DSN in Kubernetes](#set-shard_main_dsn-in-kubernetes)
- [Production Kubernetes Notes](#production-kubernetes-notes)

---

<a id="why-this-project-uses-postgresql-and-timescaledb"></a>
## Why This Project Uses PostgreSQL and TimescaleDB

The project stores a lot of timestamped market data:

- trades
- order-book depth updates
- open interest
- funding rates
- liquidations

PostgreSQL gives us a reliable SQL database, mature tooling, schemas, constraints, and very good Rust support through SQLx.

TimescaleDB runs on top of PostgreSQL and adds the stuff that is particularly useful for this kind of data:

- time-based hypertables
- automatic chunking
- compression
- retention policies
- efficient time-range queries

The main reason the database exists here is **historical storage and bot bootstrapping**.

A trading bot may start and need the previous few hours or days of data to rebuild things such as:

```text
moving averages
rolling volatility
order-flow state
funding history
open-interest state
feature windows
model inputs
indicator state
```

After that state has been rebuilt, the bot can continue processing live data without constantly querying the database.

So the database is basically:

```text
Market data
    │
    ▼
PostgreSQL + TimescaleDB
    │
    ├── durable historical storage
    ├── research / querying
    └── bootstrap bot state after startup
```

PostgreSQL is free, open source, extremely common, and there is a huge amount of tooling and knowledge around it.

That also makes the project easier to run than if it required some unusual distributed database just to get started.

---

<a id="is-it-enough-for-large-amounts-of-market-data"></a>
## Is It Enough for Large Amounts of Market Data?

For a home server, research machine, small Kubernetes cluster, or medium-sized trading project, PostgreSQL + TimescaleDB is usually more than enough.

Storing **terabytes of historical data** is completely realistic if the machine has enough disk and the database is configured sensibly.

TimescaleDB helps because the tables are divided into time chunks instead of behaving like one gigantic table.

Conceptually:

```text
trades hypertable
│
├── chunk: day 1
├── chunk: day 2
├── chunk: day 3
├── chunk: day 4
└── ...
```

A query for one day therefore does not need to treat years of data as one giant block.

Older chunks can also be compressed automatically.

For this project's intended use, that is a very useful tradeoff:

```text
simple PostgreSQL
        +
TimescaleDB time-series features
        =
a lot of storage capability
without a huge infrastructure stack
```

Performance will obviously still depend on:

- disk speed
- available RAM
- write rate
- indexes
- chunk sizes
- compression
- retention
- query design

Having 5 TB of data on a fast NVMe machine with enough RAM is a very different situation from putting the same database on a slow HDD with 4 GB of memory.

---

<a id="where-it-starts-to-struggle"></a>
## Where It Starts to Struggle

The main limitation is that this is **not a magically distributed database**.

A normal PostgreSQL/TimescaleDB deployment still revolves around a primary PostgreSQL server.

That is perfectly fine for this project, but eventually one machine becomes the limit.

You may need something larger when:

- one server can no longer keep up with the write rate
- the database becomes too large or expensive for one machine
- you need automatic write sharding across many servers
- you need huge analytical scans across petabytes of history
- many heavy research workloads compete with live ingestion
- you need multi-region database infrastructure
- you need much more advanced automatic failover and replication

At that point, a larger architecture might look something like:

```text
Recent / operational data
        │
        ▼
PostgreSQL + TimescaleDB

Very old historical data
        │
        ▼
columnar files / object storage / analytical database
```

But there is little reason to start there.

For bot development, research, home infrastructure, and medium-sized market-data collection, PostgreSQL + TimescaleDB gives a lot of capability while staying relatively simple.

---

<a id="retention-compression-and-chunk-settings"></a>
## Retention, Compression, and Chunk Settings

The storage strategy is configured in:

```text
dbsetup.sql
```

The current defaults are:

```text
Retention:
    3 months

Compress data after:
    6 hours

Chunk intervals:
    trades          1 day
    depth deltas    6 hours
    open interest   1 day
    funding         30 days
    liquidations    1 day

Additional symbol/time indexes:
    disabled by default
```

The Bybit setup currently overrides some of those defaults:

```text
Retention:
    3 days

Compress data after:
    1 hour

Chunk intervals:
    trades          12 hours
    depth deltas    6 hours
    open interest   1 day
    funding         1 day
    liquidations    12 hours
```

These settings are intentionally easy to change.

For example:

```sql
SELECT create_exchange_hypertables(
  p_exchange           => 'bybit_linear',
  p_retention          => INTERVAL '30 days',
  p_compress_after     => INTERVAL '6 hours',
  p_chunk_trades       => INTERVAL '1 day',
  p_chunk_depth        => INTERVAL '6 hours',
  p_chunk_oi           => INTERVAL '1 day',
  p_chunk_funding      => INTERVAL '7 days',
  p_chunk_liquidations => INTERVAL '1 day',
  p_create_indexes     => TRUE
);
```

So if you have plenty of disk and want more historical data:

```sql
p_retention => INTERVAL '1 year'
```

If the database is only needed to bootstrap one day of bot state:

```sql
p_retention => INTERVAL '3 days'
```

The correct values depend heavily on how much data each exchange produces and what the bots actually need.

Smaller chunks are useful for very high-volume tables, but making them unnecessarily tiny creates more database objects and overhead.

Larger chunks are simpler but require more memory and disk work per chunk.

### Optional Indexes

Setting:

```sql
p_create_indexes => TRUE
```

creates additional:

```text
(symbol, time DESC)
```

indexes.

These can be useful when repeatedly asking questions like:

```sql
Give me BTCUSDT trades from the last 24 hours.
```

The tradeoff is that indexes consume disk and make writes slightly more expensive.

### Changing Existing Policies

The setup only creates compression and retention policies when they do not already exist.

That means editing:

```text
dbsetup.sql
```

and running it again does **not necessarily replace existing TimescaleDB policies**.

For an already initialized database, existing policies may need to be altered or removed before creating new ones.

---

<a id="local-setup"></a>
## Local Setup

This example uses PostgreSQL 16 on Ubuntu/Debian.

<a id="install-postgresql-and-timescaledb"></a>
### Install PostgreSQL and TimescaleDB

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib curl

curl -s https://packagecloud.io/install/repositories/timescale/timescaledb/script.deb.sh \
  | sudo bash

sudo apt update
sudo apt install -y timescaledb-2-postgresql-16 timescaledb-tools

sudo timescaledb-tune

sudo systemctl restart postgresql
sudo systemctl enable postgresql

psql --version
pg_isready
```

`timescaledb-tune` adjusts PostgreSQL configuration for TimescaleDB.

Restart PostgreSQL afterwards.

---

<a id="create-your-user-and-database"></a>
### Create Your User and Database

Choose your own:

```text
USERNAME
PASSWORD
DATABASE_NAME
```

Open PostgreSQL:

```bash
sudo -u postgres psql
```

Create the application user and database:

```sql
CREATE ROLE USERNAME WITH LOGIN PASSWORD 'PASSWORD';

CREATE DATABASE DATABASE_NAME OWNER USERNAME;

\c DATABASE_NAME

CREATE EXTENSION IF NOT EXISTS timescaledb;

\q
```

For example:

```sql
CREATE ROLE fintick WITH LOGIN PASSWORD 'replace_this_password';

CREATE DATABASE fintickstreams OWNER fintick;

\c fintickstreams

CREATE EXTENSION IF NOT EXISTS timescaledb;
```

Test it:

```bash
psql -h 127.0.0.1 -U USERNAME -d DATABASE_NAME
```

---

<a id="run-the-database-setup-files"></a>
### Run the Database Setup Files

From the project root:

```bash
PGPASSWORD="PASSWORD" psql \
  -v ON_ERROR_STOP=1 \
  -h 127.0.0.1 \
  -U USERNAME \
  -d DATABASE_NAME \
  -f db/dbsetup.sql

PGPASSWORD="PASSWORD" psql \
  -v ON_ERROR_STOP=1 \
  -h 127.0.0.1 \
  -U USERNAME \
  -d DATABASE_NAME \
  -f db/registry.sql
```

`dbsetup.sql` creates the market-data schemas, tables, TimescaleDB hypertables, compression settings, and retention policies.

The exchange schemas are deterministic:

```text
ex_binance_linear
ex_hyperliquid_perp
ex_bybit_linear
```

`registry.sql` creates:

```text
mini_fintickstreams.stream_registry
```

The PostgreSQL username and database name are chosen during installation.

The application schema:

```text
mini_fintickstreams
```

is different. That name is part of the project and is created automatically by `registry.sql`.

The registry stores things such as:

```text
stream identity
enabled / disabled state
flush size
flush interval
database write controls
chunk size
hard batch limits
timestamps
```

This allows the runtime to restore configured streams and their state after a restart.

---

<a id="set-shard_main_dsn"></a>
### Set `SHARD_MAIN_DSN`

This variable is required:

```text
SHARD_MAIN_DSN
```

The Timescale configuration contains:

```toml
[[shards]]
id = "shard0"
dsn_env = "SHARD_MAIN_DSN"
```

So the TOML does not contain the actual database credentials.

Instead it says:

```text
Get the database connection URL from SHARD_MAIN_DSN.
```

Set it:

```bash
export SHARD_MAIN_DSN="postgresql://USERNAME:PASSWORD@127.0.0.1:5432/DATABASE_NAME"
```

For example:

```bash
export SHARD_MAIN_DSN="postgresql://fintick:replace_this_password@127.0.0.1:5432/fintickstreams"
```

Test exactly what SQLx will use:

```bash
psql "$SHARD_MAIN_DSN"
```

To keep the variable across terminal sessions:

```bash
echo 'export SHARD_MAIN_DSN="postgresql://USERNAME:PASSWORD@127.0.0.1:5432/DATABASE_NAME"' \
  >> ~/.bashrc

source ~/.bashrc
```

If the password contains characters with special meaning inside URLs, URL-encode them before putting the password into the DSN.

The local connection path is:

```text
mini-fintickstreams
        │
        ▼
SHARD_MAIN_DSN
        │
        ▼
postgresql://user:password@127.0.0.1:5432/database
        │
        ▼
SQLx
        │
        ▼
PostgreSQL + TimescaleDB
```

---

<a id="kubernetes-setup"></a>
## Kubernetes Setup

`mini-fintickstreams` was built with Kubernetes deployment in mind.

There are already Kubernetes YAML files under:

```text
k8s/
```

These should be treated as **working templates**, not as universal production configs.

They represent a setup that worked during development, but things such as:

```text
namespace
storage class
database credentials
service names
resource limits
container image
persistent volume size
```

will usually need to be changed for another cluster.

The existing application Deployment already expects its configuration to be mounted under:

```text
/etc/mini-fintickstreams
```

and loads environment variables from a Kubernetes Secret.

The repository also already has application `Deployment`, `ConfigMap`, `Secret`, and `Service` templates.

Running the Rust application itself on Kubernetes is relatively simple.

Running PostgreSQL correctly is more involved because a database needs:

- persistent storage
- stable networking
- initialization
- backups
- recovery planning

For a home cluster or development environment, a single-node StatefulSet is a reasonable starting point.

---

<a id="kubernetes-networking-is-different"></a>
### Kubernetes Networking Is Different

Locally we use:

```text
127.0.0.1:5432
```

Inside Kubernetes that usually does **not** work.

Inside the application Pod:

```text
127.0.0.1
```

means:

```text
this application Pod
```

not the PostgreSQL Pod.

Kubernetes workloads normally communicate through Services.

If the database Service is called:

```text
timescaledb
```

then the application can connect using:

```text
timescaledb:5432
```

from the same namespace.

Conceptually:

```text
mini-fintickstreams Pod
        │
        │ timescaledb:5432
        ▼
Kubernetes Service
        │
        ▼
TimescaleDB Pod
```

If the database lives in another namespace, use the full Kubernetes DNS name:

```text
timescaledb.NAMESPACE.svc.cluster.local
```

The repository's existing Secret template currently contains a database host such as:

```text
pg-postgresql
```

That name came from the Kubernetes setup used when those templates were created. It is **not a required name**.

Use whatever Service name exists in your cluster.

---

<a id="create-database-credentials"></a>
### Create Database Credentials

Choose your namespace and database credentials:

```bash
export NS="mini-fintickstreams"

export DB_USER="choose_a_username"
export DB_PASSWORD="choose_a_password"
export DB_NAME="choose_a_database"
```

Create the namespace:

```bash
kubectl create namespace "$NS" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Create the database credentials:

```bash
kubectl -n "$NS" create secret generic timescaledb-auth \
  --from-literal=POSTGRES_USER="$DB_USER" \
  --from-literal=POSTGRES_PASSWORD="$DB_PASSWORD" \
  --from-literal=POSTGRES_DB="$DB_NAME" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

For a small private cluster, using the initial PostgreSQL user for the application is fine.

For a serious production deployment, separate database administration and application roles are better.

---

<a id="example-timescaledb-statefulset"></a>
### Example TimescaleDB StatefulSet

A database should use persistent storage, so a StatefulSet is a much better fit than a normal Deployment.

Example template:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: timescaledb
spec:
  selector:
    app: timescaledb
  ports:
    - name: postgres
      port: 5432
      targetPort: postgres

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: timescaledb

spec:
  serviceName: timescaledb
  replicas: 1

  selector:
    matchLabels:
      app: timescaledb

  template:
    metadata:
      labels:
        app: timescaledb

    spec:
      terminationGracePeriodSeconds: 60

      containers:
        - name: timescaledb
          image: timescale/timescaledb:latest-pg16
          imagePullPolicy: IfNotPresent

          ports:
            - name: postgres
              containerPort: 5432

          env:
            - name: POSTGRES_USER
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_USER

            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_PASSWORD

            - name: POSTGRES_DB
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_DB

            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata

          readinessProbe:
            exec:
              command:
                - /bin/sh
                - -c
                - pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
            periodSeconds: 5
            timeoutSeconds: 3

          livenessProbe:
            exec:
              command:
                - /bin/sh
                - -c
                - pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
            periodSeconds: 15
            timeoutSeconds: 5
            failureThreshold: 6

          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data

  volumeClaimTemplates:
    - metadata:
        name: data

      spec:
        accessModes:
          - ReadWriteOnce

        resources:
          requests:
            storage: 100Gi
```

The important parts here are:

```text
StatefulSet
    └── keeps stable database identity

Service
    └── gives the database a stable network name

PersistentVolumeClaim
    └── keeps the actual database after a Pod restart
```

Apply it:

```bash
kubectl -n "$NS" apply -f k8s/timescaledb.yaml

kubectl -n "$NS" rollout status statefulset/timescaledb

kubectl -n "$NS" get pods
kubectl -n "$NS" get services
kubectl -n "$NS" get pvc
```

For a real long-running deployment, pin an exact TimescaleDB image version instead of relying forever on:

```text
latest-pg16
```

---

<a id="initialize-the-kubernetes-database"></a>
### Initialize the Kubernetes Database

The same:

```text
dbsetup.sql
registry.sql
```

files still need to run.

One convenient option is to put them in a ConfigMap:

```bash
kubectl -n "$NS" create configmap timescaledb-schema \
  --from-file=dbsetup.sql \
  --from-file=registry.sql \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

Then run them from a Kubernetes Job.

Example:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: timescaledb-init

spec:
  backoffLimit: 6

  template:
    spec:
      restartPolicy: Never

      containers:
        - name: database-init
          image: postgres:16

          env:
            - name: PGHOST
              value: timescaledb

            - name: PGPORT
              value: "5432"

            - name: PGUSER
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_USER

            - name: PGPASSWORD
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_PASSWORD

            - name: PGDATABASE
              valueFrom:
                secretKeyRef:
                  name: timescaledb-auth
                  key: POSTGRES_DB

          command:
            - /bin/sh
            - -ec

          args:
            - |
              until pg_isready \
                -h "$PGHOST" \
                -p "$PGPORT" \
                -U "$PGUSER" \
                -d "$PGDATABASE"
              do
                sleep 2
              done

              psql -v ON_ERROR_STOP=1 -f /sql/dbsetup.sql
              psql -v ON_ERROR_STOP=1 -f /sql/registry.sql

          volumeMounts:
            - name: schema
              mountPath: /sql
              readOnly: true

      volumes:
        - name: schema
          configMap:
            name: timescaledb-schema
```

Run it:

```bash
kubectl -n "$NS" delete job timescaledb-init \
  --ignore-not-found

kubectl -n "$NS" apply \
  -f k8s/timescaledb-init-job.yaml

kubectl -n "$NS" wait \
  --for=condition=complete \
  job/timescaledb-init \
  --timeout=5m

kubectl -n "$NS" logs job/timescaledb-init
```

The important line is:

```text
PGHOST=timescaledb
```

because:

```text
timescaledb
```

is the Kubernetes Service name.

If your Service has another name, change it.

---

<a id="set-shard_main_dsn-in-kubernetes"></a>
### Set `SHARD_MAIN_DSN` in Kubernetes

Locally the DSN looks like:

```text
postgresql://USERNAME:PASSWORD@127.0.0.1:5432/DATABASE_NAME
```

In Kubernetes it becomes something like:

```text
postgresql://USERNAME:PASSWORD@timescaledb:5432/DATABASE_NAME
```

The important difference is simply:

```text
Local
    127.0.0.1

Kubernetes
    DATABASE_SERVICE_NAME
```

The repository already contains a Kubernetes Secret template for `SHARD_MAIN_DSN`.

Those YAML files are examples from a setup that worked during development, so modify them for your own:

```text
username
password
database
namespace
Service name
```

For example, if your Service is called:

```text
timescaledb
```

the DSN becomes:

```text
postgresql://USERNAME:PASSWORD@timescaledb:5432/DATABASE_NAME?sslmode=disable
```

Create or update the application's database Secret:

```bash
kubectl -n "$NS" create secret generic mini-fintickstreams-secrets \
  --from-literal=SHARD_MAIN_DSN="postgresql://${DB_USER}:${DB_PASSWORD}@timescaledb:5432/${DB_NAME}?sslmode=disable" \
  --dry-run=client \
  -o yaml \
  | kubectl apply -f -
```

The final connection path becomes:

```text
mini-fintickstreams Pod
        │
        │ SHARD_MAIN_DSN
        ▼
timescaledb:5432
        │
        ▼
Kubernetes Service
        │
        ▼
TimescaleDB Pod
        │
        ▼
PersistentVolume
```

---

<a id="production-kubernetes-notes"></a>
## Production Kubernetes Notes

The single-node StatefulSet above is useful for:

```text
home Kubernetes clusters
development
research
private infrastructure
small deployments
```

It is **not automatically a highly available PostgreSQL cluster**.

Kubernetes can restart the database Pod and reconnect its persistent volume, but Kubernetes alone does not magically provide:

- PostgreSQL replication
- leader election
- automatic primary promotion
- point-in-time recovery
- tested backups
- multi-zone database failover
- database-aware upgrades

Also, do not assume this:

```yaml
replicas: 1
```

can simply become:

```yaml
replicas: 3
```

and suddenly give you a PostgreSQL cluster.

It does not.

```text
3 independent PostgreSQL Pods
        ≠
replicated PostgreSQL cluster
```

For serious infrastructure, use a PostgreSQL operator or managed PostgreSQL/TimescaleDB service that handles replication, backups, failover, and upgrades properly.

The application itself does not really care where PostgreSQL lives.

As long as:

```text
SHARD_MAIN_DSN
```

points to a working PostgreSQL + TimescaleDB endpoint, SQLx can connect to it.

For the intended scope of this project—market-data collection, trading research, bot bootstrapping, home infrastructure, and medium-sized deployments—the simpler setup is usually more than enough.
