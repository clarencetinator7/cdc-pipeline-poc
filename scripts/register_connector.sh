#!/usr/bin/env bash
# register_connector.sh
# Posts the Debezium connector config to the Kafka Connect REST API.
# For use in WSL or Git Bash.
#
# Usage: bash scripts/register_connector.sh

set -euo pipefail

CONNECT_URL="http://localhost:8083/connectors"
CONNECTOR_JSONC="$(dirname "$0")/../connect/connector.jsonc"

echo "Registering Debezium SQL Server connector..."

# Strip // comment lines from the .jsonc file before POSTing — Kafka Connect requires valid JSON.
STRIPPED_JSON=$(grep -v '^\s*//' "$CONNECTOR_JSONC")

HTTP_STATUS=$(curl -s -o /tmp/connect_response.json -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  --data "$STRIPPED_JSON" \
  "$CONNECT_URL")

if [ "$HTTP_STATUS" -eq 201 ]; then
    echo "Connector registered!"
    python3 -m json.tool /tmp/connect_response.json
elif [ "$HTTP_STATUS" -eq 409 ]; then
    echo "Connector already exists (HTTP 409)."
    echo "To replace it:"
    echo "  curl -X DELETE http://localhost:8083/connectors/sqlserver-cdc-connector"
    echo "Then re-run this script."
else
    echo "Error (HTTP $HTTP_STATUS):"
    cat /tmp/connect_response.json
    exit 1
fi
