#!/usr/bin/env bash
# =============================================================================
# event_hub_setup.sh – Create and configure Azure Event Hub
# =============================================================================
# Creates an Event Hub namespace with the sensor-events entity, consumer
# groups, and SAS policies.
#
# Usage:
#   export AZURE_RESOURCE_GROUP="sensor-anomaly-rg"
#   export AZURE_REGION="eastus"
#   bash event_hub_setup.sh
# =============================================================================
set -euo pipefail

RED='\033[0;31m'  GREEN='\033[0;32m'  YELLOW='\033[1;33m'  NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
AZURE_RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-sensor-anomaly-rg}"
AZURE_REGION="${AZURE_REGION:-eastus}"
EVENT_HUB_NAMESPACE="${EVENT_HUB_NAMESPACE:-sensor-anomaly-eh}"
EVENT_HUB_NAME="${EVENT_HUB_NAME:-sensor-events}"
EH_SKU="${EH_SKU:-Standard}"          # Basic | Standard | Premium
EH_CAPACITY="${EH_CAPACITY:-2}"        # Throughput units (Standard SKU)
EH_PARTITION_COUNT="${EH_PARTITION_COUNT:-4}"
EH_RETENTION_DAYS="${EH_RETENTION_DAYS:-7}"
CONSUMER_GROUP_ADF="${CONSUMER_GROUP_ADF:-adf-consumer}"
CONSUMER_GROUP_APP="${CONSUMER_GROUP_APP:-app-consumer}"

# ── Prerequisite check ────────────────────────────────────────────────────────
command -v az >/dev/null 2>&1 || error "Azure CLI not found."

info "Creating Event Hub namespace '$EVENT_HUB_NAMESPACE' (SKU=$EH_SKU, capacity=$EH_CAPACITY) …"
az eventhubs namespace create \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name           "$EVENT_HUB_NAMESPACE" \
    --location       "$AZURE_REGION" \
    --sku            "$EH_SKU" \
    --capacity       "$EH_CAPACITY" \
    --enable-auto-inflate true \
    --maximum-throughput-units 10 \
    --output none
info "Namespace created ✅"

info "Creating Event Hub entity '$EVENT_HUB_NAME' …"
az eventhubs eventhub create \
    --resource-group      "$AZURE_RESOURCE_GROUP" \
    --namespace-name      "$EVENT_HUB_NAMESPACE" \
    --name                "$EVENT_HUB_NAME" \
    --partition-count     "$EH_PARTITION_COUNT" \
    --message-retention   "$EH_RETENTION_DAYS" \
    --output none
info "Event Hub entity created ✅"

info "Creating consumer groups …"
az eventhubs eventhub consumer-group create \
    --resource-group  "$AZURE_RESOURCE_GROUP" \
    --namespace-name  "$EVENT_HUB_NAMESPACE" \
    --eventhub-name   "$EVENT_HUB_NAME" \
    --name            "$CONSUMER_GROUP_ADF" \
    --output none

az eventhubs eventhub consumer-group create \
    --resource-group  "$AZURE_RESOURCE_GROUP" \
    --namespace-name  "$EVENT_HUB_NAMESPACE" \
    --eventhub-name   "$EVENT_HUB_NAME" \
    --name            "$CONSUMER_GROUP_APP" \
    --output none
info "Consumer groups created ✅"

info "Creating SAS authorization rule for publisher …"
az eventhubs namespace authorization-rule create \
    --resource-group   "$AZURE_RESOURCE_GROUP" \
    --namespace-name   "$EVENT_HUB_NAMESPACE" \
    --name             "PublisherPolicy" \
    --rights           Send \
    --output none

info "Creating SAS authorization rule for consumer …"
az eventhubs namespace authorization-rule create \
    --resource-group   "$AZURE_RESOURCE_GROUP" \
    --namespace-name   "$EVENT_HUB_NAMESPACE" \
    --name             "ConsumerPolicy" \
    --rights           Listen \
    --output none
info "Authorization rules created ✅"

# ── Print connection strings ───────────────────────────────────────────────────
info "Retrieving connection strings …"
PUBLISHER_CONN="$(az eventhubs namespace authorization-rule keys list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --namespace-name "$EVENT_HUB_NAMESPACE" \
    --name           "PublisherPolicy" \
    --query          primaryConnectionString \
    --output         tsv)"

CONSUMER_CONN="$(az eventhubs namespace authorization-rule keys list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --namespace-name "$EVENT_HUB_NAMESPACE" \
    --name           "ConsumerPolicy" \
    --query          primaryConnectionString \
    --output         tsv)"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Event Hub Setup Complete  ✅                                ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Namespace      : $EVENT_HUB_NAMESPACE"
echo "║  Entity         : $EVENT_HUB_NAME"
echo "║  Partitions     : $EH_PARTITION_COUNT"
echo "║  Retention      : ${EH_RETENTION_DAYS} days"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
warn "Save these connection strings securely (e.g. in Azure Key Vault):"
echo "  Publisher  : $PUBLISHER_CONN"
echo "  Consumer   : $CONSUMER_CONN"
echo ""
info "Add to your environment:"
echo "  export EVENT_HUB_CONNECTION_STR=\"$PUBLISHER_CONN\""
echo "  export EVENT_HUB_NAME=\"$EVENT_HUB_NAME\""
