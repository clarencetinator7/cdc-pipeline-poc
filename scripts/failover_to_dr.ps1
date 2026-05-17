# failover_to_dr.ps1
# Simulates a full DR failover:
#   1. Switches kafka-connect to the DR Kafka cluster (kafka-dr:29093)
#   2. Re-registers the Debezium connector with schema history pointing at DR
#
# Prerequisites:
#   - docker compose up -d is running
#   - MirrorMaker 2 has had time to replicate connect-offsets and schema-changes.CdcDemo to DR
#   - Primary Kafka can be stopped before or after running this script
#
# Usage: .\scripts\failover_to_dr.ps1

$ConnectUrl   = "http://localhost:8083"
$ConnectorName = "sqlserver-cdc-connector"

# ── STEP 1: Switch kafka-connect to DR ────────────────────────────────────────
Write-Host ""
Write-Host "Step 1: Switching kafka-connect to kafka-dr:29093..." -ForegroundColor Cyan

$env:KAFKA_CONNECT_BOOTSTRAP = "kafka-dr:29093"
docker compose up -d --force-recreate kafka-connect

# ── STEP 2: Wait for kafka-connect to be ready ────────────────────────────────
Write-Host ""
Write-Host "Step 2: Waiting for kafka-connect to be healthy..." -ForegroundColor Cyan

$maxAttempts = 24  # 2 minutes
$attempt = 0
do {
    Start-Sleep 5
    $attempt++
    $ready = $false
    try {
        $null = Invoke-RestMethod "$ConnectUrl/connectors"
        $ready = $true
    } catch {}
    Write-Host "  [$attempt/$maxAttempts] waiting..."
} while (-not $ready -and $attempt -lt $maxAttempts)

if (-not $ready) {
    Write-Host "kafka-connect did not become healthy in time. Check: docker logs kafka-connect" -ForegroundColor Red
    exit 1
}

Write-Host "  kafka-connect is up." -ForegroundColor Green

# ── STEP 3: Re-register connector with schema history pointing at DR ───────────
# We do NOT replicate connect-configs — instead we re-register here with one
# change: schema.history.internal.kafka.bootstrap.servers → kafka-dr:29093.
# kafka-connect will find Debezium's last LSN in the replicated connect-offsets
# topic on DR and resume CDC from exactly that position.
Write-Host ""
Write-Host "Step 3: Registering connector on DR cluster..." -ForegroundColor Cyan

# Strip // comment lines from the .jsonc file (same as register_connector.ps1)
$ConnectorJson = (Get-Content -Path "$PSScriptRoot\..\connect\connector.jsonc") `
    | Where-Object { $_ -notmatch '^\s*//' } `
    | Out-String

$Connector = $ConnectorJson | ConvertFrom-Json

# Point schema history at the DR cluster instead of the (down) primary
$Connector.config.'schema.history.internal.kafka.bootstrap.servers' = 'kafka-dr:29093'

$Body = $Connector | ConvertTo-Json -Depth 10

try {
    $Response = Invoke-RestMethod `
        -Method Post `
        -Uri "$ConnectUrl/connectors" `
        -ContentType "application/json" `
        -Body $Body

    Write-Host "  Connector registered: $($Response.name)" -ForegroundColor Green
}
catch {
    $StatusCode = $_.Exception.Response.StatusCode.value__
    if ($StatusCode -eq 409) {
        Write-Host "  Connector already exists on DR — skipping registration." -ForegroundColor Yellow
    } else {
        Write-Host "  Error (HTTP $StatusCode): $($_.ErrorDetails.Message)" -ForegroundColor Red
        exit 1
    }
}

# ── DONE ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Failover complete." -ForegroundColor Green
Write-Host ""
Write-Host "Debezium is resuming CDC from its last committed LSN on the DR cluster."
Write-Host "New SQL Server changes will now flow to kafka-dr."
Write-Host ""
Write-Host "To switch consumer.py to DR:"
Write-Host '  $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"'
Write-Host "  python consumer/consumer.py"
Write-Host ""
Write-Host "Check connector status:"
Write-Host "  Invoke-RestMethod http://localhost:8083/connectors/$ConnectorName/status"
