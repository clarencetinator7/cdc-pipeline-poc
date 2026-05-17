# Solution Summary: Dual Debezium + MM2 DR Architecture

## Architecture

```
                    On-Prem SQL Server (CDC enabled)
                              │
                ┌─────────────┴─────────────┐
                │ JDBC T-Log poll           │ JDBC T-Log poll
                ▼                           ▼
        Debezium Primary              Debezium DR
        (RUNNING)                     (PAUSED — standby)
        eu-west-1                     eu-west-2
                │                           │
                ▼                           ▼
        MSK Primary  ◄────── MM2 ──────►  MSK DR
        (eu-west-1)      (bidirectional)  (eu-west-2)
                │                           │
                └─────────────┬─────────────┘
                              ▼
                        Databricks
                  (idempotent MERGE on LSN)
```

## Component Responsibilities

**Debezium Primary (eu-west-1)** — Active connector polling SQL Server CDC transaction log, publishing change events to MSK Primary. Stores its source offset (LSN position) in the `connect-offsets` internal topic.

**Debezium DR (eu-west-2)** — Pre-deployed but paused. Same connector config as primary, version-controlled in Git, ready to resume on a single API call. Will read its starting LSN from the replicated offset state, not from scratch.

**MM2 (MirrorMaker 2)** — Runs continuously, self-hosted on EKS. Three connectors:

- `MirrorSourceConnector` — replicates data topics bidirectionally.
- `MirrorCheckpointConnector` — replicates and translates consumer group offsets (for Databricks).
- `MirrorHeartbeatConnector` — emits heartbeats to measure replication lag (your real RPO indicator).
- Also configured to mirror `connect-offsets` so Debezium DR can resume from primary's last known LSN.

**MSK Primary / MSK DR** — Standard MSK clusters in both regions. Both always running.

**Databricks Sink** — Consumes from MSK Primary normally. On failover, repoints to MSK DR using offsets translated by `MirrorCheckpointConnector`. Uses `MERGE INTO ... WHEN MATCHED AND incoming.lsn > target.lsn` so any replay is absorbed safely.

## Normal Operation Flow

1. SQL Server commits a transaction → entry in CDC transaction log.
2. Debezium Primary polls the log → publishes event to MSK Primary topic.
3. MM2 replicates the event to MSK DR (seconds of lag).
4. MM2 replicates Debezium's offset state to MSK DR's `connect-offsets`.
5. Databricks consumes from MSK Primary → MERGEs into target table by LSN.

## Failover Flow (eu-west-1 down)

Triggered by automated runbook:

1. Health checks confirm primary outage (multiple signals, not transient).
2. Verify MM2 replication lag is acceptable (heartbeat topic).
3. Stop/fence Debezium Primary if reachable (prevent split-brain).
4. Resume Debezium DR via Connect API — it reads last LSN from mirrored offsets and continues from there.
5. Repoint Databricks bootstrap servers to MSK DR.
6. Verify event flow resumes; alert completion.

**RTO:** seconds to minutes. **RPO:** seconds (MM2 replication lag).

## Failback Flow (eu-west-1 restored)

1. Allow MM2 to drain DR → Primary direction (data written during outage flows back).
2. Pause Debezium DR.
3. Resume Debezium Primary — it picks up from the offset state that's now been replicated back from DR.
4. Repoint Databricks to MSK Primary.
5. Any overlap/replay is absorbed by the idempotent MERGE.

## Why This Works

| Original Problem | How It's Solved |
|---|---|
| Gap risk from `snapshot.mode=no_data` | DR connector resumes from replicated LSN, not "now" |
| Lost offsets during DR | MM2 mirrors `connect-offsets` continuously |
| Manual connector recreation | DR connector pre-deployed, paused; resume is one API call |
| Failback symmetry | Bidirectional MM2 + idempotent MERGE handles both directions |
| Duplicate/replay safety | LSN-based MERGE in Databricks makes at-least-once safe |

## Non-Negotiable Supporting Pieces

- **Idempotent MERGE in Databricks** — the safety net that makes everything else tolerable.
- **Schema Registry replicated cross-region** — DR consumers must be able to deserialize.
- **Heartbeat monitoring** — replication lag is your live RPO; alert when it exceeds threshold.
- **SQL Server CDC retention** longer than worst-case failover detection time, so the LSN you want to resume from still exists.
- **Quarterly DR drills** — untested DR is a hypothesis, not a plan.

## Hosting Decisions

- **MM2:** self-hosted Kafka Connect on EKS in eu-west-1 (and a mirror set in eu-west-2 for symmetry). Avoids MSK Connect markup, keeps replication inside AWS to avoid double egress.
- **Debezium connectors:** Kafka Connect clusters in each region — primary running, DR paused.

## Cost Footprint

Roughly 2x your current single-region MSK/Connect spend, dominated by:

- Cross-region MSK egress (~$0.02/GB).
- Duplicate MSK cluster in eu-west-2.
- MM2 Connect workers on EKS (compute is minor).

That's the price of RPO≈0. If that's too rich, Option B (S3 offset snapshots, no MM2) achieves most of the safety at a fraction of the cost — the idempotent MERGE does the heavy lifting in both designs.
