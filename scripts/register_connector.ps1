# register_connector.ps1
# Posts the Debezium connector config to the Kafka Connect REST API.
# Run from the project root AFTER all services are healthy.
#
# Usage: .\scripts\register_connector.ps1

$ConnectUrl    = "http://localhost:8083/connectors"

# Strip // comment lines from the .jsonc file before POSTing — Kafka Connect requires valid JSON.
$ConnectorJson = (Get-Content -Path "$PSScriptRoot\..\connect\connector.jsonc") `
    | Where-Object { $_ -notmatch '^\s*//' } `
    | Out-String

Write-Host "Registering Debezium SQL Server connector..." -ForegroundColor Cyan

try {
    $Response = Invoke-RestMethod `
        -Method Post `
        -Uri $ConnectUrl `
        -ContentType "application/json" `
        -Body $ConnectorJson

    Write-Host "Connector registered!" -ForegroundColor Green
    Write-Host "Name       : $($Response.name)"
    Write-Host "Status URL : http://localhost:8083/connectors/$($Response.name)/status"
}
catch {
    $StatusCode = $_.Exception.Response.StatusCode.value__
    $ErrorBody  = $_.ErrorDetails.Message

    if ($StatusCode -eq 409) {
        Write-Host "Connector already exists (HTTP 409)." -ForegroundColor Yellow
        Write-Host "To replace it, delete first:"
        Write-Host "  Invoke-RestMethod -Method Delete -Uri 'http://localhost:8083/connectors/sqlserver-cdc-connector'"
        Write-Host "Then re-run this script."
    }
    else {
        Write-Host "Error (HTTP $StatusCode):" -ForegroundColor Red
        Write-Host $ErrorBody
    }
}
