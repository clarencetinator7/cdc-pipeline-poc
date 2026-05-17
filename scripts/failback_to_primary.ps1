# failback_to_primary.ps1
# Returns CDC processing to the primary cluster after a DR failover.
#
# Steps:
#   1. Confirm primary Kafka is up
#   2. Wait for MM2 dr->primary replication to catch up (all DR events arrive on primary)
#   3. Pause the DR connector
#   4. Resume the primary connector — it reads DR's replicated LSN from connect-offsets
#      on primary and resumes without a snapshot
#   5. Print instructions to switch consumer.py back
#
# Usage: .\scripts\failback_to_primary.ps1

$PrimaryConnectUrl = "http://localhost:8083/connectors"
$DrConnectUrl      = "http://localhost:8084/connectors"
$ConnectorName     = "sqlserver-cdc-connector"
$Topic             = "cdc.CdcDemo.dbo.customers"

# ── STEP 1: Confirm primary Kafka is reachable ─────────────────────────────────
Write-Host ""
Write-Host "Step 1: Checking primary Kafka..." -ForegroundColor Cyan

try {
    docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw }
    Write-Host "  Primary Kafka is up." -ForegroundColor Green
}
catch {
    Write-Host "  Primary Kafka is not reachable. Start it first: docker start kafka" -ForegroundColor Red
    exit 1
}

# ── STEP 2: Wait for DR→primary replication to catch up ───────────────────────
Write-Host ""
Write-Host "Step 2: Waiting for MM2 to replicate DR events back to primary..." -ForegroundColor Cyan
Write-Host "  (Comparing end offsets on both clusters — polling every 5 seconds)"

$maxAttempts = 24  # 2 minutes
$attempt = 0
$synced = $false

do {
    $attempt++
    Start-Sleep 5

    $primaryOffset = docker exec kafka kafka-run-class kafka.tools.GetOffsetShell `
        --broker-list localhost:29092 --topic $Topic --time -1 2>$null |
        Select-String -Pattern ":\d+$" | ForEach-Object { ($_ -split ":")[-1] } |
        Measure-Object -Sum | Select-Object -ExpandProperty Sum

    $drOffset = docker exec kafka-dr kafka-run-class kafka.tools.GetOffsetShell `
        --broker-list localhost:29093 --topic $Topic --time -1 2>$null |
        Select-String -Pattern ":\d+$" | ForEach-Object { ($_ -split ":")[-1] } |
        Measure-Object -Sum | Select-Object -ExpandProperty Sum

    Write-Host "  [$attempt/$maxAttempts] primary=$primaryOffset  dr=$drOffset"

    if ($null -ne $primaryOffset -and $null -ne $drOffset -and $primaryOffset -ge $drOffset) {
        $synced = $true
    }
} while (-not $synced -and $attempt -lt $maxAttempts)

if (-not $synced) {
    Write-Host "  Replication did not catch up in time. Check: docker logs mirrormaker2" -ForegroundColor Red
    exit 1
}

Write-Host "  Replication caught up." -ForegroundColor Green

# ── STEP 3: Pause DR connector ────────────────────────────────────────────────
Write-Host ""
Write-Host "Step 3: Pausing DR connector..." -ForegroundColor Cyan

Invoke-RestMethod -Method Put -Uri "$DrConnectUrl/$ConnectorName/pause" | Out-Null
Write-Host "  DR connector paused." -ForegroundColor Green

# ── STEP 4: Resume primary connector ──────────────────────────────────────────
# The primary connector reads connect-offsets on primary Kafka. MM2 has replicated
# DR's latest LSN there, so Debezium resumes from that position — no snapshot.
Write-Host ""
Write-Host "Step 4: Resuming primary connector..." -ForegroundColor Cyan

Invoke-RestMethod -Method Put -Uri "$PrimaryConnectUrl/$ConnectorName/resume" | Out-Null

Start-Sleep 3
$status = Invoke-RestMethod -Uri "$PrimaryConnectUrl/$ConnectorName/status"
Write-Host "  Connector state : $($status.connector.state)"
Write-Host "  Task state      : $($status.tasks[0].state)" -ForegroundColor Green

# ── DONE ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Failback complete." -ForegroundColor Green
Write-Host "Debezium is back on the primary cluster, resuming from the last DR offset."
Write-Host ""
Write-Host "Switch consumer.py back to primary:"
Write-Host '  $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"'
Write-Host "  python consumer/consumer.py"
