# failover_to_dr.ps1
# Triggers a DR failover by resuming the pre-registered (paused) Debezium connector
# on kafka-connect-dr.
#
# How the offset works:
#   MM2 continuously replicates connect-offsets primary→DR. When the DR connector
#   resumes, it finds the primary Debezium's last committed LSN already in DR's
#   connect-offsets and resumes CDC from that position — no snapshot, no gap.
#
# Prerequisites:
#   - docker compose up -d is running
#   - register_connector_dr.ps1 has been run (DR connector is PAUSED on port 8084)
#   - MM2 has had time to replicate connect-offsets to DR (give it ~30 seconds after startup)
#
# Usage: .\scripts\failover_to_dr.ps1

$DrConnectUrl  = "http://localhost:8084/connectors"
$ConnectorName = "sqlserver-cdc-connector"

Write-Host ""
Write-Host "Resuming DR Debezium connector on kafka-connect-dr..." -ForegroundColor Cyan

try {
    Invoke-RestMethod -Method Put -Uri "$DrConnectUrl/$ConnectorName/resume" | Out-Null
    Write-Host "DR connector resumed." -ForegroundColor Green
}
catch {
    Write-Host "Error: $($_.ErrorDetails.Message)" -ForegroundColor Red
    Write-Host "Is kafka-connect-dr running? Check: docker compose ps" -ForegroundColor Yellow
    exit 1
}

# Give Debezium a moment to start up and commit its first offset
Start-Sleep 3

$status = Invoke-RestMethod -Uri "$DrConnectUrl/$ConnectorName/status"
Write-Host "Connector state : $($status.connector.state)"
Write-Host "Task state      : $($status.tasks[0].state)"
Write-Host ""
Write-Host "Debezium is now reading from SQL Server and writing to kafka-dr."
Write-Host "New CDC events flow through the DR cluster."
Write-Host ""
Write-Host "Switch consumer.py to DR:"
Write-Host '  $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"'
Write-Host "  python consumer/consumer.py"
Write-Host ""
Write-Host "When ready to fail back:"
Write-Host "  .\scripts\failback_to_primary.ps1"
