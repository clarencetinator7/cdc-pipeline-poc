# register_connector_dr.ps1
# Registers the Debezium connector on the DR kafka-connect instance (port 8084)
# then immediately pauses it.
#
# During normal operation this connector stays PAUSED — it does not poll SQL Server
# and does not write to kafka-dr. On failover, resume it with:
#   Invoke-RestMethod -Method Put http://localhost:8084/connectors/sqlserver-cdc-connector/resume
#
# Usage: .\scripts\register_connector_dr.ps1

$ConnectUrl    = "http://localhost:8084/connectors"
$ConnectorName = "sqlserver-cdc-connector"

# Strip // comment lines from the .jsonc file before POSTing.
$ConnectorJson = (Get-Content -Path "$PSScriptRoot\..\connect\connector-dr.jsonc") `
    | Where-Object { $_ -notmatch '^\s*//' } `
    | Out-String

Write-Host "Registering DR Debezium connector on kafka-connect-dr (port 8084)..." -ForegroundColor Cyan

try {
    $Response = Invoke-RestMethod `
        -Method Post `
        -Uri $ConnectUrl `
        -ContentType "application/json" `
        -Body $ConnectorJson

    Write-Host "Connector registered: $($Response.name)" -ForegroundColor Green
}
catch {
    $StatusCode = $_.Exception.Response.StatusCode.value__

    if ($StatusCode -eq 409) {
        Write-Host "Connector already exists on DR (HTTP 409) — skipping registration." -ForegroundColor Yellow
    }
    else {
        Write-Host "Error (HTTP $StatusCode): $($_.ErrorDetails.Message)" -ForegroundColor Red
        exit 1
    }
}

# Pause immediately so it does not compete with the primary connector.
Write-Host "Pausing DR connector..." -ForegroundColor Cyan

Invoke-RestMethod `
    -Method Put `
    -Uri "$ConnectUrl/$ConnectorName/pause" | Out-Null

Write-Host "DR connector is PAUSED and ready for failover." -ForegroundColor Green
Write-Host ""
Write-Host "To trigger failover:"
Write-Host "  .\scripts\failover_to_dr.ps1"
