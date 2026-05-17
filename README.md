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
┌─────────────────────────┐       ┌─────────────────────────┐
│  kafka-connect          │       │  kafka-connect-dr        │
│  Debezium  [RUNNING]    │       │  Debezium  [PAUSED]      │
│  → writes to primary    │       │  → writes to DR          │
│  port 8083              │       │  port 8084               │
└──────────┬──────────────┘       └──────────┬───────────────┘
           │                                 │
           ▼                                 ▼
   Kafka PRIMARY (KRaft)  ◄──MM2──►  Kafka DR (KRaft)
        :9092                              :9093
           │                                 │
           └──────────┬──────────────────────┘
                      ▼
                consumer.py
          (points at whichever cluster is active)
```

On failover: `resume` the DR connector (one API call). On failback: `pause` DR, `resume` primary.

### Service Map

| Service | Image | Host Port |
|---|---|---|
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 |
| Kafka (primary, KRaft) | `confluentinc/cp-kafka:7.6.1` | 9092 (host), 29092 (internal) |
| Kafka DR (KRaft) | `confluentinc/cp-kafka:7.6.1` | 9093 (host), 29093 (internal) |
| MirrorMaker 2 | `confluentinc/cp-kafka:7.6.1` | — |
| Kafka Connect (primary) | `quay.io/debezium/connect:3.0.0.Final` | 8083 |
| Kafka Connect (DR) | `quay.io/debezium/connect:3.0.0.Final` | 8084 |

Both Kafka clusters run in **KRaft mode** — no ZooKeeper required.

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
    ├── kafka-connect       ←  waits for kafka AND sqlserver-init
    └── mirrormaker2        ←  waits for kafka AND kafka-dr
kafka-dr (KRaft, healthy)
    ├── kafka-connect-dr    ←  waits for kafka-dr AND sqlserver-init
    └── mirrormaker2
```

### 2. Verify all services are healthy

```powershell
docker compose ps
```

Expected: all long-running services show `(healthy)`, `sqlserver-init` shows `Exited (0)`.

### 3. Register the Debezium connectors

```powershell
# Primary connector — RUNNING, writes to kafka:29092
.\scripts\register_connector.ps1

# DR connector — registered then immediately PAUSED, ready for failover
.\scripts\register_connector_dr.ps1
```

### 4. Confirm connector states

```powershell
Invoke-RestMethod http://localhost:8083/connectors/sqlserver-cdc-connector/status  # state: RUNNING
Invoke-RestMethod http://localhost:8084/connectors/sqlserver-cdc-connector/status  # state: PAUSED
```

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

The consumer reads `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) from the environment — switch it to `localhost:9093` to point at DR without code changes.

---

## Disaster Recovery

### How it works

MirrorMaker 2 runs bidirectional replication between both clusters. Each direction replicates:

| Topic | Why it's needed |
|---|---|
| `cdc.CdcDemo.dbo.customers` | The CDC event data itself |
| `connect-offsets` | Debezium's last-committed SQL Server LSN — lets it resume from the exact position on either cluster |
| `schema-changes.CdcDemo` | Debezium's DDL history — needed to reconstruct table schemas at every LSN on restart |

`IdentityReplicationPolicy` keeps topic names identical on both clusters — neither the consumer nor kafka-connect needs different config on failover.

MM2's **circular replication protection** stamps every replicated message with a `__mm2_source_cluster_alias` header. A message that originated on primary and was copied to DR will not be sent back to primary — MM2 skips it automatically.

> **Production note:** AWS MSK Replication is managed MirrorMaker 2 under the hood. The active-passive Debezium pattern (one connector running, one paused) is the standard approach for multi-region Kafka Connect DR.

### Normal state

```
kafka-connect      → Debezium RUNNING  → writes to primary Kafka
kafka-connect-dr   → Debezium PAUSED   → ready, not writing
MM2                → primary ↔ DR      → continuous bidirectional sync
```

### Verify replication is active

```powershell
docker exec kafka-dr kafka-topics --bootstrap-server localhost:29093 --list
# expect: cdc.CdcDemo.dbo.customers, connect-offsets, schema-changes.CdcDemo
```

### Failover (primary Kafka goes down)

```powershell
# Simulate disaster
docker stop kafka

# One command — resumes the pre-registered DR connector.
# Debezium reads the replicated LSN from connect-offsets on kafka-dr and picks up
# exactly where the primary connector left off. No snapshot, no gap.
.\scripts\failover_to_dr.ps1

# Switch consumer to DR
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
python consumer/consumer.py
```

### Failback (primary Kafka restored)

```powershell
# Bring primary back
docker start kafka

# Waits for MM2 dr->primary replication to catch up, then swaps the active connector.
.\scripts\failback_to_primary.ps1

# Switch consumer back to primary
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python consumer/consumer.py
```

The failback script polls end offsets on both clusters and only swaps connectors once primary has all DR events — preventing any gap.

---

## File Structure

```
poc-cdc-pipeline/
├── docker-compose.yml                # All services (KRaft, no ZooKeeper)
├── sqlserver/
│   ├── init.sql                      # Creates CdcDemo, enables CDC, seeds 3 rows
│   └── debug.sql                     # INSERT / UPDATE / DELETE / RESET for testing
├── connect/
│   ├── connector.jsonc               # Primary Debezium connector config
│   ├── connector-dr.jsonc            # DR Debezium connector config (schema history → kafka-dr)
│   └── mm2.properties                # MirrorMaker 2 bidirectional replication config
├── scripts/
│   ├── register_connector.ps1        # Register primary connector (PowerShell)
│   ├── register_connector.sh         # Register primary connector (Bash/WSL)
│   ├── register_connector_dr.ps1     # Register DR connector and immediately pause it
│   ├── failover_to_dr.ps1            # Resume DR connector (one API call)
│   └── failback_to_primary.ps1       # Wait for MM2 sync, swap connectors back
└── consumer/
    ├── consumer.py                   # CDC event pretty-printer (env-var configurable)
    └── requirements.txt              # confluent-kafka==2.4.0
```

---

## Connector Configuration

Both connector configs live in [connect/](connect/) in JSONC format. The registration scripts strip comments before POSTing.

Key settings:

| Property | Primary (`connector.jsonc`) | DR (`connector-dr.jsonc`) |
|---|---|---|
| `schema.history.internal.kafka.bootstrap.servers` | `kafka:29092` | `kafka-dr:29093` |
| `snapshot.mode` | `initial` | `initial` (skipped — offset already exists from MM2 replication) |
| `table.include.list` | `dbo.customers` | `dbo.customers` |
| `topic.prefix` | `cdc` | `cdc` |

All other properties (SQL Server connection, converters, table filter) are identical.

---

## Useful Commands

**Check connector states:**

```powershell
Invoke-RestMethod http://localhost:8083/connectors/sqlserver-cdc-connector/status  # primary
Invoke-RestMethod http://localhost:8084/connectors/sqlserver-cdc-connector/status  # DR
```

**List topics on primary / DR:**

```powershell
docker exec kafka    kafka-topics --bootstrap-server localhost:29092 --list
docker exec kafka-dr kafka-topics --bootstrap-server localhost:29093 --list
```

**Compare end offsets (replication lag check):**

```powershell
docker exec kafka    kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic cdc.CdcDemo.dbo.customers --time -1
docker exec kafka-dr kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29093 --topic cdc.CdcDemo.dbo.customers --time -1
```

**Consume a topic raw:**

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:29092 `
  --topic cdc.CdcDemo.dbo.customers `
  --from-beginning
```

**Check logs:**

```powershell
docker logs sqlserver-init    # Confirm CDC jobs started and rows seeded
docker logs kafka-connect     # Primary connector errors
docker logs kafka-connect-dr  # DR connector errors
docker logs mirrormaker2      # Replication activity
```

**Reset the primary connector** (re-runs snapshot):

```powershell
Invoke-RestMethod -Method Delete -Uri 'http://localhost:8083/connectors/sqlserver-cdc-connector'
.\scripts\register_connector.ps1
```

**Tear down:**

```powershell
docker compose down      # Stops containers, preserves volumes
docker compose down -v   # Full reset including volumes
```
