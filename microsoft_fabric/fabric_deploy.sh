#!/usr/bin/env bash
# =============================================================================
# fabric_deploy.sh – Deploy all Microsoft Fabric components
# =============================================================================
# This script orchestrates the full deployment of the machine sensor anomaly
# detection system to Microsoft Azure / Fabric:
#
#   1. Validate prerequisites (Azure CLI, Docker)
#   2. Create Azure Resource Group
#   3. Deploy Event Hub namespace and entity
#   4. Deploy Azure Data Factory via ARM template
#   5. Build and push Docker image for REST API
#   6. Deploy REST API to Azure Container Instances
#   7. Configure monitoring alerts
#
# Usage:
#   export AZURE_SUBSCRIPTION_ID="<your-subscription-id>"
#   export AZURE_RESOURCE_GROUP="sensor-rg"
#   export AZURE_REGION="eastus"
#   bash fabric_deploy.sh
#
# Environment Variables (required):
#   AZURE_SUBSCRIPTION_ID   Azure subscription ID
#   AZURE_RESOURCE_GROUP    Resource group name (will be created)
#   AZURE_REGION            Azure region (e.g. eastus, westeurope)
#   ACR_NAME                Azure Container Registry name
#   FABRIC_WORKSPACE_ID     Microsoft Fabric Workspace GUID
#   FABRIC_LAKEHOUSE_ID     Microsoft Fabric Lakehouse GUID
#
# Environment Variables (optional – have defaults):
#   EVENT_HUB_NAMESPACE     Event Hub namespace name
#   EVENT_HUB_NAME          Event Hub entity name
#   ADF_NAME                Azure Data Factory name
#   ACI_NAME                Azure Container Instance name
#   API_IMAGE_TAG           Docker image tag (default: latest)
# =============================================================================
set -euo pipefail

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'  GREEN='\033[0;32m'  YELLOW='\033[1;33m'  NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Defaults ──────────────────────────────────────────────────────────────────
AZURE_SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-}"
AZURE_RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-sensor-anomaly-rg}"
AZURE_REGION="${AZURE_REGION:-eastus}"
ACR_NAME="${ACR_NAME:-sensoranomalyacr}"
FABRIC_WORKSPACE_ID="${FABRIC_WORKSPACE_ID:-}"
FABRIC_LAKEHOUSE_ID="${FABRIC_LAKEHOUSE_ID:-}"

EVENT_HUB_NAMESPACE="${EVENT_HUB_NAMESPACE:-sensor-anomaly-eh}"
EVENT_HUB_NAME="${EVENT_HUB_NAME:-sensor-events}"
ADF_NAME="${ADF_NAME:-sensor-anomaly-adf}"
ACI_NAME="${ACI_NAME:-sensor-api}"
API_IMAGE_TAG="${API_IMAGE_TAG:-latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Prerequisite checks ───────────────────────────────────────────────────────
info "Checking prerequisites …"
command -v az     >/dev/null 2>&1 || error "Azure CLI (az) not found. Install: https://aka.ms/installazurecli"
command -v docker >/dev/null 2>&1 || error "Docker not found. Install: https://docs.docker.com/get-docker/"

[[ -z "$AZURE_SUBSCRIPTION_ID" ]] && error "AZURE_SUBSCRIPTION_ID is not set."
[[ -z "$FABRIC_WORKSPACE_ID"  ]] && warn  "FABRIC_WORKSPACE_ID not set – ADF deployment will be skipped."
[[ -z "$FABRIC_LAKEHOUSE_ID"  ]] && warn  "FABRIC_LAKEHOUSE_ID not set – ADF deployment will be skipped."

# ── Azure login check ─────────────────────────────────────────────────────────
info "Verifying Azure CLI login …"
az account show --subscription "$AZURE_SUBSCRIPTION_ID" >/dev/null 2>&1 \
    || error "Not logged in to Azure. Run: az login"
az account set --subscription "$AZURE_SUBSCRIPTION_ID"
info "Active subscription: $(az account show --query name -o tsv)"

# ── Resource Group ────────────────────────────────────────────────────────────
info "Creating resource group '$AZURE_RESOURCE_GROUP' …"
az group create \
    --name     "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_REGION" \
    --output   none
info "Resource group ready ✅"

# ── Event Hub ─────────────────────────────────────────────────────────────────
info "Setting up Event Hub …"
bash "${SCRIPT_DIR}/event_hub_setup.sh"
info "Event Hub ready ✅"

# ── Azure Data Factory ────────────────────────────────────────────────────────
if [[ -n "$FABRIC_WORKSPACE_ID" && -n "$FABRIC_LAKEHOUSE_ID" ]]; then
    info "Deploying Azure Data Factory '$ADF_NAME' …"
    EH_CONN_STR="$(az eventhubs namespace authorization-rule keys list \
        --resource-group   "$AZURE_RESOURCE_GROUP" \
        --namespace-name   "$EVENT_HUB_NAMESPACE" \
        --name             "RootManageSharedAccessKey" \
        --query            primaryConnectionString \
        --output           tsv)"

    az deployment group create \
        --resource-group    "$AZURE_RESOURCE_GROUP" \
        --template-file     "${SCRIPT_DIR}/data_factory_pipeline.json" \
        --parameters \
            factoryName="$ADF_NAME" \
            location="$AZURE_REGION" \
            eventHubConnectionString="$EH_CONN_STR" \
            eventHubName="$EVENT_HUB_NAME" \
            fabricWorkspaceId="$FABRIC_WORKSPACE_ID" \
            fabricLakehouseId="$FABRIC_LAKEHOUSE_ID" \
        --output none
    info "Data Factory deployed ✅"
else
    warn "Skipping Data Factory deployment (FABRIC_WORKSPACE_ID / FABRIC_LAKEHOUSE_ID not set)"
fi

# ── Container Registry ────────────────────────────────────────────────────────
info "Creating Azure Container Registry '$ACR_NAME' …"
az acr create \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name           "$ACR_NAME" \
    --sku            Basic \
    --output         none

ACR_LOGIN_SERVER="$(az acr show --name "$ACR_NAME" --query loginServer -o tsv)"
info "ACR login server: $ACR_LOGIN_SERVER"

# ── Build & Push Docker Image ─────────────────────────────────────────────────
info "Building Docker image …"
az acr build \
    --registry        "$ACR_NAME" \
    --image           "sensor-api:${API_IMAGE_TAG}" \
    --file            "${SCRIPT_DIR}/Dockerfile" \
    "${SCRIPT_DIR}"
info "Image pushed to ACR ✅"

# ── Deploy to ACI ─────────────────────────────────────────────────────────────
info "Deploying REST API to ACI …"
bash "${SCRIPT_DIR}/deploy_to_aci.sh"
info "REST API deployed ✅"

# ── Power BI ──────────────────────────────────────────────────────────────────
info "Configuring Power BI workspace …"
bash "${SCRIPT_DIR}/powerbi_setup.sh" || warn "Power BI setup skipped (run manually)"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           DEPLOYMENT COMPLETE  ✅                            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Resource Group : $AZURE_RESOURCE_GROUP"
echo "║  Event Hub      : $EVENT_HUB_NAMESPACE/$EVENT_HUB_NAME"
echo "║  Data Factory   : $ADF_NAME"
echo "║  Container Reg  : $ACR_LOGIN_SERVER"
echo "║  REST API (ACI) : $ACI_NAME"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
info "Next: run 'python event_hub_publisher.py' to start streaming sensor data."
