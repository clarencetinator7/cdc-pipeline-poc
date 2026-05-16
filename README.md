# CDC Pipeline — Local Study Environment

A fully containerized CDC (Change Data Capture) pipeline for local exploration, mirroring a production setup:

**Production:** SQL Server → Debezium → AWS MSK → Databricks  
**This repo:** SQL Server → Debezium → Kafka → Python consumer

---

## Architecture

```
SQL Server (CDC enabled on dbo.customers)
        │  T-Log polling via JDBC
        ▼
Kafka Connect + Debezium SQL Server Connector
        │  JSON messages, topic: cdc.CdcDemo.dbo.customers
        ▼
Apache Kafka (single broker + Zookeeper)
        │  confluent-kafka consumer
        ▼
consumer.py  ─  pretty-prints op / before / after / changed fields
```

### Service Map

| Service | Image | Host Port |
|---|---|---|
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 |
| Zookeeper | `confluentinc/cp-zookeeper:7.6.1` | 2181 |
| Kafka | `confluentinc/cp-kafka:7.6.1` | 9092 (host), 29092 (internal) |
| Kafka Connect + Debezium | `quay.io/debezium/connect:3.0.0.Final` | 8083 |

---

## Prerequisites

- Docker Desktop (with Linux containers)
- Python 3.9+ (for the consumer)
- PowerShell 5.1+ or Git Bash (for the registration script)

---

## Quick Start

### 1. Start the stack

```powershell
docker compose up -d
```

Docker Compose enforces this startup order via health checks:

```
sqlserver (healthy)
    └── sqlserver-init  →  runs init.sql, exits 0
zookeeper (healthy)
    └── kafka (healthy)
            ├── schema-registry
            └── kafka-connect  ←  waits for kafka AND sqlserver-init
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

Connect to SQL Server from the host:

```powershell
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P 'YourStrong!Passw0rd' -No -d CdcDemo
```

Then run DML:

```sql
-- INSERT
INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES (4, 'Diana', 'Prince', 'diana@example.com', 'Themyscira');

-- UPDATE
UPDATE dbo.customers SET city = 'Metropolis' WHERE customer_id = 1;

-- DELETE
DELETE FROM dbo.customers WHERE customer_id = 4;
```

---

## Consumer Output

```
──────────────────────────────────────────────────────────────────────
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
──────────────────────────────────────────────────────────────────────
```

Operation codes: `r` = snapshot read, `c` = insert, `u` = update, `d` = delete.

---

## File Structure

```
poc-cdc-pipeline/
├── docker-compose.yml          # All 6 services with health checks
├── sqlserver/
│   └── init.sql                # Creates CdcDemo, enables CDC, seeds 3 rows
├── connect/
│   └── connector.jsonc         # Debezium connector config (commented)
├── scripts/
│   ├── register_connector.ps1  # POST connector config (PowerShell)
│   └── register_connector.sh   # POST connector config (Bash/WSL)
└── consumer/
    ├── consumer.py             # CDC event pretty-printer
    └── requirements.txt        # confluent-kafka==2.4.0
```

---

## Connector Configuration

The connector config lives in [connect/connector.jsonc](connect/connector.jsonc) in JSONC format (JSON with `//` comments). The registration scripts strip comments before POSTing to the Kafka Connect REST API.

Key settings:

| Property | Value | Why |
|---|---|---|
| `topic.prefix` | `cdc` | Topics named `cdc.<db>.<schema>.<table>` |
| `database.encrypt` | `false` | Docker uses a self-signed cert — JDBC rejects it by default |
| `database.trustServerCertificate` | `true` | Required alongside `encrypt=false` |
| `value.converter.schemas.enable` | `false` | Strips Connect schema envelope; consumer gets flat JSON |
| `snapshot.mode` | `initial` | Snapshots all existing rows on first start |

---

## Useful Commands

**Reset the connector** (re-runs snapshot):

```powershell
Invoke-RestMethod -Method Delete -Uri 'http://localhost:8083/connectors/sqlserver-cdc-connector'
.\scripts\register_connector.ps1
```

**List Kafka topics:**

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

**Consume the CDC topic raw:**

```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:29092 `
  --topic cdc.CdcDemo.dbo.customers `
  --from-beginning
```

**Check logs:**

```powershell
docker logs sqlserver-init   # Confirm CDC jobs started and rows seeded
docker logs kafka-connect    # Watch for connector errors
```

**Tear down:**

```powershell
docker compose down          # Stops containers, preserves volumes
docker compose down -v       # Full reset including volumes
```
