# CDC Pipeline — Local Study Environment

A fully containerized CDC (Change Data Capture) pipeline for local exploration, mirroring a production setup:

**Production:** SQL Server → Debezium → AWS MSK (primary + DR) → Databricks  
**This repo:** SQL Server → Debezium → Kafka (primary + DR) → Python consumer

---

## Architecture

```
SQL Server (CDC enabled on dbo.customers)
        │  T-Log polling via JDBC
        ▼
Kafka Connect + Debezium SQL Server Connector
        │  JSON messages, topic: cdc.CdcDemo.dbo.customers
        ▼
Kafka PRIMARY (KRaft, :9092)
        │
        ├── consumer.py  ─  pretty-prints op / before / after / changed fields
        │
        └── MirrorMaker 2  ──────────────────────────────────────────────────┐
              replicates: CDC topic + connect-offsets + schema-changes        │
                                                                              ▼
                                                                    Kafka DR (KRaft, :9093)
                                                                              │
                                                               consumer.py  (on failover)
```

### Service Map

| Service | Image | Host Port |
|---|---|---|
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 |
| Kafka (primary, KRaft) | `confluentinc/cp-kafka:7.6.1` | 9092 (host), 29092 (internal) |
| Kafka DR (KRaft) | `confluentinc/cp-kafka:7.6.1` | 9093 (host), 29093 (internal) |
| MirrorMaker 2 | `confluentinc/cp-kafka:7.6.1` | — |
| Kafka Connect + Debezium | `quay.io/debezium/connect:3.0.0.Final` | 8083 |

Both Kafka clusters run in **KRaft mode** — no ZooKeeper required. Each broker is its own controller.

---

## Prerequisites

- Docker Desktop (with Linux containers)
- Python 3.9+ (for the consumer)
- PowerShell 5.1+ or Git Bash (for the scripts)

---

## Quick Start

### 1. Start the stack

```powershell
docker compose up -d
```

Startup order enforced by health checks:

```
sqlserver (healthy)
    └── sqlserver-init  →  runs init.sql, exits 0
kafka (KRaft, healthy)
    ├── kafka-connect   ←  waits for kafka AND sqlserver-init
    └── mirrormaker2    ←  waits for kafka AND kafka-dr
kafka-dr (KRaft, healthy)
    └── mirrormaker2
```

### 2. Verify all services are healthy

```powershell
docker compose ps
```

Expected: all long-running services show `(healthy)`, `sqlserver-init` shows `Exited (0)`.

### 3. Register the Debezium connector

```powershell
.\scripts\register_connector.ps1
```

Or from WSL/Git Bash:

```bash
bash scripts/register_connector.sh
```

### 4. Confirm the connector is running

```powershell
Invoke-RestMethod http://localhost:8083/connectors/sqlserver-cdc-connector/status
```

`state` should be `RUNNING`.

### 5. Run the Python consumer

```powershell
pip install -r consumer/requirements.txt
python consumer/consumer.py
```

On first run you'll see 3 `SNAPSHOT READ` events for the seeded rows (Alice, Bob, Charlie). After that, any INSERT/UPDATE/DELETE on `dbo.customers` appears within ~1-2 seconds.

---

## Generating CDC Events

Use the pre-built debug script in SQL Server:

```powershell
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P 'YourStrong!Passw0rd' -No -d CdcDemo
```

Then paste statements from `sqlserver/debug.sql` — it has sections for INSERT, UPDATE, DELETE, multi-row operations, and a RESET back to the seeded state.

---

## Consumer Output

```
──────────────────────────────────────────────────────────────────────────────
  Operation  : UPDATE  [u]
  Event time : 2026-05-16 10:23:45 UTC
  Table      : dbo.customers
  LSN        : 00000031:00000b80:0003
··············································································
  BEFORE:
    customer_id: 1
    first_name: Alice
    city: New York
    ...

  AFTER:
    customer_id: 1
    first_name: Alice
    city: Metropolis  ← changed from: New York
    ...
──────────────────────────────────────────────────────────────────────────────
```

Operation codes: `r` = snapshot read, `c` = insert, `u` = update, `d` = delete.

The consumer reads from `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` environment variables (defaulting to the primary cluster). This allows switching to DR without code changes.

---

## Disaster Recovery

### How it works

MirrorMaker 2 (MM2) continuously replicates three topics from the primary Kafka to the DR Kafka:

| Topic | Why it's needed |
|---|---|
| `cdc.CdcDemo.dbo.customers` | The CDC event data itself |
| `connect-offsets` | Debezium's last-committed SQL Server LSN — lets it resume from the exact position after failover |
| `schema-changes.CdcDemo` | Debezium's DDL history — needed to reconstruct table schemas at every LSN on restart |

`IdentityReplicationPolicy` is used so topic names are **identical** on both clusters. Neither the consumer nor kafka-connect need different config on failover.

> **Note:** This mirrors how AWS MSK Replication works — it is managed MirrorMaker 2 under the hood. Running MM2 yourself on EC2/ECS against MSK clusters is the self-hosted equivalent and uses the same configuration.

### Verify replication is active

```powershell
docker exec kafka-dr kafka-topics --bootstrap-server localhost:29093 --list
# expect: cdc.CdcDemo.dbo.customers, connect-offsets, schema-changes.CdcDemo
```

### Simulating a disaster

```powershell
docker stop kafka
```

The primary Kafka is down. The consumer will error and exit. New SQL Server changes continue accumulating in the CDC change tables — SQL Agent doesn't care that Kafka is down.

### Manual failover — step by step

**Step 1 — Switch kafka-connect to the DR cluster:**

```powershell
$env:KAFKA_CONNECT_BOOTSTRAP = "kafka-dr:29093"
docker compose up -d kafka-connect
```

**Step 2 — Wait for kafka-connect to be healthy:**

```powershell
Invoke-RestMethod http://localhost:8083/connectors
# returns [] once ready
```

**Step 3 — Re-register the Debezium connector pointing schema history at DR:**

The only change from the original config is `schema.history.internal.kafka.bootstrap.servers` → `kafka-dr:29093`. Debezium will find its last LSN in the replicated `connect-offsets` topic and resume CDC from that position — no re-snapshot.

```powershell
$body = @{
  name = "sqlserver-cdc-connector"
  config = @{
    "connector.class"                                    = "io.debezium.connector.sqlserver.SqlServerConnector"
    "tasks.max"                                          = "1"
    "topic.prefix"                                       = "cdc"
    "database.hostname"                                  = "sqlserver"
    "database.port"                                      = "1433"
    "database.user"                                      = "sa"
    "database.password"                                  = "YourStrong!Passw0rd"
    "database.names"                                     = "CdcDemo"
    "database.encrypt"                                   = "false"
    "database.trustServerCertificate"                    = "true"
    "table.include.list"                                 = "dbo.customers"
    "schema.history.internal.kafka.bootstrap.servers"    = "kafka-dr:29093"
    "schema.history.internal.kafka.topic"                = "schema-changes.CdcDemo"
    "key.converter"                                      = "org.apache.kafka.connect.json.JsonConverter"
    "key.converter.schemas.enable"                       = "false"
    "value.converter"                                    = "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable"                     = "false"
    "include.schema.changes"                             = "true"
    "snapshot.mode"                                      = "initial"
  }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType "application/json" -Body $body
```

**Step 4 — Confirm Debezium resumed (no snapshot):**

```powershell
Invoke-RestMethod http://localhost:8083/connectors/sqlserver-cdc-connector/status
# state: RUNNING — consumer should NOT see new SNAPSHOT READ events
```

**Step 5 — Switch consumer to DR:**

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
python consumer/consumer.py
```

New SQL Server changes now flow through the DR cluster end-to-end.

### Automated failover

The script `scripts/failover_to_dr.ps1` runs all five steps above automatically:

```powershell
.\scripts\failover_to_dr.ps1
```

---

## File Structure

```
poc-cdc-pipeline/
├── docker-compose.yml              # All services with health checks (KRaft, no ZooKeeper)
├── sqlserver/
│   ├── init.sql                    # Creates CdcDemo, enables CDC, seeds 3 rows
│   └── debug.sql                   # INSERT / UPDATE / DELETE / RESET statements for testing
├── connect/
│   ├── connector.jsonc             # Debezium connector config (commented)
│   └── mm2.properties              # MirrorMaker 2 replication config
├── scripts/
│   ├── register_connector.ps1      # POST connector config (PowerShell)
│   ├── register_connector.sh       # POST connector config (Bash/WSL)
│   └── failover_to_dr.ps1          # Full DR failover automation (PowerShell)
└── consumer/
    ├── consumer.py                 # CDC event pretty-printer (env-var configurable)
    └── requirements.txt            # confluent-kafka==2.4.0
```

---

## Connector Configuration

The connector config lives in [connect/connector.jsonc](connect/connector.jsonc) in JSONC format (JSON with `//` comments). The registration scripts strip comments before POSTing to the Kafka Connect REST API.

Key settings:

| Property | Value | Why |
|---|---|---|
| `topic.prefix` | `cdc` | Topics named `cdc.<db>.<schema>.<table>` |
| `table.include.list` | `dbo.customers` | Schema.table format only — database is set separately in `database.names` |
| `database.encrypt` | `false` | Docker uses a self-signed cert — JDBC rejects it by default |
| `database.trustServerCertificate` | `true` | Required alongside `encrypt=false` |
| `value.converter.schemas.enable` | `false` | Strips Connect schema envelope; consumer gets flat JSON |
| `snapshot.mode` | `initial` | Snapshots all existing rows on first start |
| `schema.history.internal.kafka.topic` | `schema-changes.CdcDemo` | DDL history replayed on connector restart to reconstruct schemas |

---

## Useful Commands

**Reset the connector** (re-runs snapshot):

```powershell
Invoke-RestMethod -Method Delete -Uri 'http://localhost:8083/connectors/sqlserver-cdc-connector'
.\scripts\register_connector.ps1
```

**List topics on primary / DR:**

```powershell
docker exec kafka    kafka-topics --bootstrap-server localhost:29092 --list
docker exec kafka-dr kafka-topics --bootstrap-server localhost:29093 --list
```

**Consume a topic raw** (useful for debugging message shape):

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:29092 `
  --topic cdc.CdcDemo.dbo.customers `
  --from-beginning
```

**Check MirrorMaker 2 replication lag:**

```powershell
docker logs mirrormaker2 --tail 50
```

**Check logs:**

```powershell
docker logs sqlserver-init   # Confirm CDC jobs started and rows seeded
docker logs kafka-connect    # Watch for connector errors
docker logs mirrormaker2     # Watch replication activity
```

**Tear down:**

```powershell
docker compose down      # Stops containers, preserves volumes
docker compose down -v   # Full reset including volumes (required when switching ZK → KRaft)
```
