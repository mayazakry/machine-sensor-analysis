#!/usr/bin/env bash
# =============================================================================
# deploy_to_aci.sh – Deploy REST API to Azure Container Instances
# =============================================================================
# Builds the Docker image (if not already in ACR), then deploys the sensor
# anomaly detection REST API as an ACI container group.
#
# Usage:
#   export AZURE_RESOURCE_GROUP="sensor-anomaly-rg"
#   export ACR_NAME="sensoranomalyacr"
#   export MODEL_PATH="models/model_latest.pkl"
#   bash deploy_to_aci.sh
# =============================================================================
set -euo pipefail

RED='\033[0;31m'  GREEN='\033[0;32m'  YELLOW='\033[1;33m'  NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
AZURE_RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-sensor-anomaly-rg}"
AZURE_REGION="${AZURE_REGION:-eastus}"
ACR_NAME="${ACR_NAME:-sensoranomalyacr}"
ACI_NAME="${ACI_NAME:-sensor-api}"
API_IMAGE_TAG="${API_IMAGE_TAG:-latest}"
ACI_CPU="${ACI_CPU:-1}"
ACI_MEMORY_GB="${ACI_MEMORY_GB:-1.5}"
ACI_PORT="${ACI_PORT:-8000}"
API_KEY="${API_KEY:-}"                          # Leave empty for no auth
MODEL_PATH="${MODEL_PATH:-models/model_latest.pkl}"
EVENT_HUB_CONNECTION_STR="${EVENT_HUB_CONNECTION_STR:-}"
TEAMS_WEBHOOK_URL="${TEAMS_WEBHOOK_URL:-}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Prerequisites ─────────────────────────────────────────────────────────────
command -v az >/dev/null 2>&1 || error "Azure CLI not found."

# ── Get ACR credentials ───────────────────────────────────────────────────────
info "Retrieving ACR credentials …"
ACR_LOGIN_SERVER="$(az acr show \
    --name            "$ACR_NAME" \
    --resource-group  "$AZURE_RESOURCE_GROUP" \
    --query           loginServer \
    --output          tsv)"

ACR_USERNAME="$(az acr credential show \
    --name   "$ACR_NAME" \
    --query  username \
    --output tsv)"

ACR_PASSWORD="$(az acr credential show \
    --name   "$ACR_NAME" \
    --query  "passwords[0].value" \
    --output tsv)"

IMAGE_FULL="${ACR_LOGIN_SERVER}/sensor-api:${API_IMAGE_TAG}"
info "Image: $IMAGE_FULL"

# ── Build environment variable list ───────────────────────────────────────────
ENV_VARS=(
    "PORT=${ACI_PORT}"
    "LOG_LEVEL=${LOG_LEVEL}"
    "MODEL_PATH=${MODEL_PATH}"
)
[[ -n "$API_KEY"                 ]] && ENV_VARS+=("API_KEY=${API_KEY}")
[[ -n "$EVENT_HUB_CONNECTION_STR" ]] && ENV_VARS+=("EVENT_HUB_CONNECTION_STR=${EVENT_HUB_CONNECTION_STR}")
[[ -n "$TEAMS_WEBHOOK_URL"       ]] && ENV_VARS+=("TEAMS_WEBHOOK_URL=${TEAMS_WEBHOOK_URL}")

# ── Deploy ACI ────────────────────────────────────────────────────────────────
info "Deploying ACI container group '$ACI_NAME' …"
az container create \
    --resource-group    "$AZURE_RESOURCE_GROUP" \
    --name              "$ACI_NAME" \
    --image             "$IMAGE_FULL" \
    --registry-login-server "$ACR_LOGIN_SERVER" \
    --registry-username "$ACR_USERNAME" \
    --registry-password "$ACR_PASSWORD" \
    --cpu               "$ACI_CPU" \
    --memory            "$ACI_MEMORY_GB" \
    --os-type           Linux \
    --ports             "$ACI_PORT" \
    --dns-name-label    "${ACI_NAME}-${AZURE_RESOURCE_GROUP}" \
    --environment-variables "${ENV_VARS[@]}" \
    --restart-policy    OnFailure \
    --location          "$AZURE_REGION" \
    --output none

# ── Retrieve public URL ────────────────────────────────────────────────────────
FQDN="$(az container show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name           "$ACI_NAME" \
    --query          ipAddress.fqdn \
    --output         tsv)"

info "Waiting for container to become healthy …"
for i in $(seq 1 12); do
    STATUS="$(az container show \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --name "$ACI_NAME" \
        --query "containers[0].instanceView.currentState.state" \
        --output tsv 2>/dev/null || echo 'Pending')"
    info "  State: $STATUS (attempt $i/12)"
    [[ "$STATUS" == "Running" ]] && break
    sleep 10
done

# ── Health check ──────────────────────────────────────────────────────────────
API_URL="http://${FQDN}:${ACI_PORT}"
info "Performing health check at $API_URL/health …"
for i in $(seq 1 5); do
    HTTP_STATUS="$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/health" || echo '000')"
    if [[ "$HTTP_STATUS" == "200" ]]; then
        info "Health check passed ✅"
        break
    fi
    warn "Health check attempt $i/5 returned HTTP $HTTP_STATUS – retrying …"
    sleep 5
done

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ACI Deployment Complete  ✅                                 ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Container  : $ACI_NAME"
echo "║  API URL    : $API_URL"
echo "║  Health     : $API_URL/health"
echo "║  Predict    : POST $API_URL/predict"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
info "Test with:"
echo "  curl -s $API_URL/health | python -m json.tool"
echo "  curl -s -X POST $API_URL/predict \\"
echo "       -H 'Content-Type: application/json' \\"
echo "       -d '{\"machine_id\":\"M1\",\"temperature\":85,\"vibration\":0.5,\"pressure\":101,\"humidity\":50,\"current\":12,\"voltage\":220}'"
