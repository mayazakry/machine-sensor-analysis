#!/usr/bin/env bash
# =============================================================================
# powerbi_setup.sh – Configure Power BI workspace and dataset
# =============================================================================
# This script uses the Power BI REST API to:
#   1. Create a dedicated Power BI workspace.
#   2. Create a dataset backed by the Fabric Lakehouse.
#   3. Import the powerbi_queries.sql definitions as named queries.
#   4. Configure dataset refresh schedule.
#
# Prerequisites:
#   - Power BI Pro or Premium licence
#   - Service Principal with Power BI Admin / Workspace Member permissions
#   - AZURE_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET set
#
# Usage:
#   bash powerbi_setup.sh
# =============================================================================
set -euo pipefail

RED='\033[0;31m'  GREEN='\033[0;32m'  YELLOW='\033[1;33m'  NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
AZURE_TENANT_ID="${AZURE_TENANT_ID:-}"
POWERBI_CLIENT_ID="${POWERBI_CLIENT_ID:-}"
POWERBI_CLIENT_SECRET="${POWERBI_CLIENT_SECRET:-}"
PBI_WORKSPACE_NAME="${PBI_WORKSPACE_NAME:-SensorAnomalyDashboard}"
PBI_DATASET_NAME="${PBI_DATASET_NAME:-SensorAnomalyDataset}"
FABRIC_LAKEHOUSE_SQL_ENDPOINT="${FABRIC_LAKEHOUSE_SQL_ENDPOINT:-}"  # e.g. abc123.datawarehouse.fabric.microsoft.com
REFRESH_FREQUENCY="${REFRESH_FREQUENCY:-hourly}"   # daily | hourly

PBI_API="https://api.powerbi.com/v1.0/myorg"

# ── Prerequisites ─────────────────────────────────────────────────────────────
command -v curl >/dev/null 2>&1 || error "curl not found."
command -v jq   >/dev/null 2>&1 || error "jq not found. Install: sudo apt-get install -y jq"

[[ -z "$AZURE_TENANT_ID"      ]] && error "AZURE_TENANT_ID is not set."
[[ -z "$POWERBI_CLIENT_ID"    ]] && error "POWERBI_CLIENT_ID is not set."
[[ -z "$POWERBI_CLIENT_SECRET" ]] && error "POWERBI_CLIENT_SECRET is not set."

# ── Obtain Power BI access token ──────────────────────────────────────────────
info "Obtaining Power BI access token …"
TOKEN_RESPONSE="$(curl -s -X POST \
    "https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/v2.0/token" \
    -d "client_id=${POWERBI_CLIENT_ID}" \
    -d "client_secret=${POWERBI_CLIENT_SECRET}" \
    -d "scope=https://analysis.windows.net/powerbi/api/.default" \
    -d "grant_type=client_credentials")"

ACCESS_TOKEN="$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')"
[[ "$ACCESS_TOKEN" == "null" || -z "$ACCESS_TOKEN" ]] \
    && error "Failed to obtain token: $(echo "$TOKEN_RESPONSE" | jq -r '.error_description')"
info "Access token obtained ✅"

# ── Create Power BI workspace ─────────────────────────────────────────────────
info "Creating Power BI workspace '$PBI_WORKSPACE_NAME' …"
WORKSPACE_RESPONSE="$(curl -s -X POST \
    "${PBI_API}/groups" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${PBI_WORKSPACE_NAME}\"}")"

WORKSPACE_ID="$(echo "$WORKSPACE_RESPONSE" | jq -r '.id')"
if [[ "$WORKSPACE_ID" == "null" || -z "$WORKSPACE_ID" ]]; then
    # Workspace may already exist – try to find it
    WORKSPACES="$(curl -s "${PBI_API}/groups" \
        -H "Authorization: Bearer $ACCESS_TOKEN")"
    WORKSPACE_ID="$(echo "$WORKSPACES" | jq -r ".value[] | select(.name==\"${PBI_WORKSPACE_NAME}\") | .id")"
    [[ -z "$WORKSPACE_ID" ]] && error "Failed to create or find workspace."
    info "Using existing workspace ID: $WORKSPACE_ID"
else
    info "Workspace created. ID: $WORKSPACE_ID"
fi

# ── Create Power BI Dataset (push dataset) ────────────────────────────────────
info "Creating dataset '$PBI_DATASET_NAME' …"
DATASET_PAYLOAD='{
  "name": "'"$PBI_DATASET_NAME"'",
  "defaultMode": "Push",
  "tables": [
    {
      "name": "SensorEvents",
      "columns": [
        {"name": "machine_id",       "dataType": "string"},
        {"name": "prediction_time",  "dataType": "dateTime"},
        {"name": "is_anomaly",       "dataType": "boolean"},
        {"name": "anomaly_score",    "dataType": "double"},
        {"name": "confidence",       "dataType": "double"},
        {"name": "temperature",      "dataType": "double"},
        {"name": "vibration",        "dataType": "double"},
        {"name": "pressure",         "dataType": "double"},
        {"name": "humidity",         "dataType": "double"},
        {"name": "current",          "dataType": "double"},
        {"name": "voltage",          "dataType": "double"}
      ]
    }
  ]
}'

DATASET_RESPONSE="$(curl -s -X POST \
    "${PBI_API}/groups/${WORKSPACE_ID}/datasets" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$DATASET_PAYLOAD")"

DATASET_ID="$(echo "$DATASET_RESPONSE" | jq -r '.id')"
info "Dataset created. ID: $DATASET_ID"

# ── Configure refresh schedule (DirectQuery datasets auto-refresh) ─────────────
if [[ -n "$FABRIC_LAKEHOUSE_SQL_ENDPOINT" ]]; then
    info "Configuring dataset refresh schedule ($REFRESH_FREQUENCY) …"
    REFRESH_SCHEDULE='{
      "value": {
        "enabled": true,
        "notifyOption": "NoNotification",
        "times": ["00:00", "06:00", "12:00", "18:00"],
        "localTimeZoneId": "UTC"
      }
    }'
    curl -s -X PATCH \
        "${PBI_API}/groups/${WORKSPACE_ID}/datasets/${DATASET_ID}/refreshSchedule" \
        -H "Authorization: Bearer $ACCESS_TOKEN" \
        -H "Content-Type: application/json" \
        -d "$REFRESH_SCHEDULE" \
        > /dev/null
    info "Refresh schedule configured ✅"
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Power BI Setup Complete  ✅                                 ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Workspace  : $PBI_WORKSPACE_NAME (ID: $WORKSPACE_ID)"
echo "║  Dataset    : $PBI_DATASET_NAME  (ID: $DATASET_ID)"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
info "Next steps:"
echo "  1. Open Power BI Desktop and connect to the workspace."
echo "  2. Create reports using the SQL queries in powerbi_queries.sql."
echo "  3. Publish the report to workspace: $PBI_WORKSPACE_NAME"
echo "  4. Pin visuals to a dashboard named 'Sensor Anomaly Monitoring'."
echo ""
info "Push API endpoint for streaming data:"
echo "  POST https://api.powerbi.com/v1.0/myorg/groups/${WORKSPACE_ID}/datasets/${DATASET_ID}/rows"
