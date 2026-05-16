# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Purpose

A local study environment replicating a production CDC pipeline: SQL Server → Debezium → Kafka → Python consumer. The production pipeline uses AWS MSK and Databricks; this version replaces those with local Docker containers and a Python script.

## Stack and Port Map

| Service | Image | Host Port |
|---|---|---|
| SQL Server | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 |
| Zookeeper | `confluentinc/cp-zookeeper:7.6.1` | 2181 |
| Kafka | `confluentinc/cp-kafka:7.6.1` | 9092 (host), 29092 (internal) |
| Kafka Connect + Debezium | `quay.io/debezium/connect:3.0.0.Final` | 8083 |

## Common Commands

**Start / stop the stack**
```powershell
docker compose up -d
docker compose down
docker compose ps        # verify all show (healthy); sqlserver-init shows Exited (0)
```

**Check logs**
```powershell
docker logs sqlserver-init   # confirm CDC jobs started and rows seeded
docker logs kafka-connect    # watch for connector errors
docker logs sqlserver        # SQL Server startup / agent status
```

**Register the Debezium connector** (after all services are healthy)
```powershell
.\scripts\register_connector.ps1

# Check connector status
Invoke-RestMethod http://localhost:8083/connectors/sqlserver-cdc-connector/status

# Delete and re-register (full reset)
Invoke-RestMethod -Method Delete http://localhost:8083/connectors/sqlserver-cdc-connector
```

**Run the Python consumer**
```powershell
pip install -r consumer/requirements.txt
python consumer/consumer.py
```

**Run SQL against SQL Server** (from host)
```powershell
docker exec -it sqlserver /opt/mssql-tools18/bin/sqlcmd `
  -S localhost -U sa -P 'YourStrong!Passw0rd' -No -d CdcDemo
```

**List Kafka topics**
```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list
```

**Consume a topic raw** (useful for debugging message shape)
```powershell
docker exec kafka kafka-console-consumer `
  --bootstrap-server localhost:29092 `
  --topic cdc.CdcDemo.dbo.customers `
  --from-beginning
```

## Architecture

### Startup dependency order

Docker Compose enforces this chain via health checks:

```
sqlserver (healthy)
    └── sqlserver-init (runs init.sql, exits 0)
zookeeper (healthy)
    └── kafka (healthy)
            ├── schema-registry
            └── kafka-connect  ← waits for BOTH kafka AND sqlserver-init
```

`kafka-connect` will not start until the database and CDC jobs are confirmed ready.

### Why `sqlserver-init` is a separate container

The `mcr.microsoft.com/mssql/server` image has no `/docker-entrypoint-initdb.d/` hook (unlike Postgres). The init container uses the same image but overrides the entrypoint to `["/bin/bash"]`, bypassing `launch_sqlservr.sh`, so it can run `sqlcmd` directly against the already-running `sqlserver` container.

### Kafka dual-listener setup

Kafka advertises two listeners so both internal and external clients resolve correctly:
- `kafka:29092` — used by Debezium (container-to-container)
- `localhost:9092` — used by `consumer.py` running on the host

### CDC event flow

1. A DML change hits `CdcDemo.dbo.customers` in SQL Server
2. SQL Agent's `cdc.CdcDemo_capture` job polls the T-Log and writes to CDC change tables
3. Debezium reads those change tables via JDBC and publishes to Kafka topic `cdc.CdcDemo.dbo.customers`
4. `consumer.py` polls that topic and pretty-prints `before`/`after` with changed fields highlighted

### Message format

The connector uses `JsonConverter` with `schemas.enable=false`. Every message value is a flat JSON object:

```json
{
  "op": "u",
  "before": { "customer_id": 1, "city": "New York", ... },
  "after":  { "customer_id": 1, "city": "Metropolis", ... },
  "source": { "table": "customers", "change_lsn": "...", ... },
  "ts_ms": 1234567890000
}
```

`op` values: `r` = snapshot read, `c` = insert, `u` = update, `d` = delete.

### Internal Kafka topics (do not consume)

- `connect-configs`, `connect-offsets`, `connect-status` — Kafka Connect worker state
- `schema-changes.CdcDemo` — Debezium DDL history, required for schema reconstruction after restart

## Key Gotchas

- **SQL Agent must be running** (`MSSQL_AGENT_ENABLED=true`). Without it, CDC is "enabled" in metadata but no capture jobs are created and no events flow.
- **`database.encrypt=false`** is required because SQL Server in Docker uses a self-signed cert that the JDBC driver rejects by default.
- **`snapshot.mode=initial`** means re-registering the connector (after deleting it) will re-snapshot all existing rows. Use `snapshot.mode=schema_only` to skip that.
- **Zookeeper healthcheck** uses `echo srvr | nc` (not `ruok`) because `ruok` is disabled by default in newer ZooKeeper; only `srvr` is whitelisted.
- **`tasks.max` must be 1** for the SQL Server connector — it does not support parallel task execution per database.
