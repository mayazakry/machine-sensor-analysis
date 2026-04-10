#!/usr/bin/env bash
# =============================================================================
# cloudrun_deploy.sh – Deploy the Sensor Anomaly Detection FastAPI to Cloud Run
#
# Usage:
#   export PROJECT_ID=your-project-id
#   export ENDPOINT_ID=1234567890
#   bash cloudrun_deploy.sh
#
# Optional overrides (all have sensible defaults):
#   SERVICE_NAME    – Cloud Run service name       (default: sensor-anomaly-api)
#   REGION          – GCP region                   (default: us-central1)
#   IMAGE_TAG       – Docker image tag             (default: latest)
#   MIN_INSTANCES   – Minimum serving instances    (default: 1)
#   MAX_INSTANCES   – Maximum serving instances    (default: 10)
#   MEMORY          – Container memory             (default: 1Gi)
#   CPU             – vCPU count                   (default: 1)
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable must be set.}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="${SERVICE_NAME:-sensor-anomaly-api}"
ENDPOINT_ID="${ENDPOINT_ID:?ERROR: ENDPOINT_ID environment variable must be set.}"
MIN_INSTANCES="${MIN_INSTANCES:-1}"
MAX_INSTANCES="${MAX_INSTANCES:-10}"
MEMORY="${MEMORY:-1Gi}"
CPU="${CPU:-1}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

REPO_NAME="sensor-anomaly-repo"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/sensor-api:${IMAGE_TAG}"

echo "========================================="
echo "  Deploying Sensor Anomaly API"
echo "  Project  : ${PROJECT_ID}"
echo "  Region   : ${REGION}"
echo "  Service  : ${SERVICE_NAME}"
echo "  Image    : ${IMAGE_URI}"
echo "========================================="

# ---------------------------------------------------------------------------
# 1. Ensure required APIs are enabled
# ---------------------------------------------------------------------------
echo ""
echo "[1/6] Enabling required Google Cloud APIs …"
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  containerregistry.googleapis.com \
  --project="${PROJECT_ID}" \
  --quiet

# ---------------------------------------------------------------------------
# 2. Create Artifact Registry repository (idempotent)
# ---------------------------------------------------------------------------
echo ""
echo "[2/6] Ensuring Artifact Registry repository exists …"
gcloud artifacts repositories describe "${REPO_NAME}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null \
|| gcloud artifacts repositories create "${REPO_NAME}" \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --description="Sensor anomaly detection service images"

# ---------------------------------------------------------------------------
# 3. Authenticate Docker with Artifact Registry
# ---------------------------------------------------------------------------
echo ""
echo "[3/6] Configuring Docker authentication …"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ---------------------------------------------------------------------------
# 4. Build and push the Docker image
# ---------------------------------------------------------------------------
echo ""
echo "[4/6] Building and pushing Docker image …"
# Run from the google_cloud/ directory so Docker context is correct
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker build \
  --tag "${IMAGE_URI}" \
  --file "${SCRIPT_DIR}/Dockerfile" \
  "${SCRIPT_DIR}"

docker push "${IMAGE_URI}"

# ---------------------------------------------------------------------------
# 5. Create or update the Cloud Run service
# ---------------------------------------------------------------------------
echo ""
echo "[5/6] Deploying to Cloud Run …"
gcloud run deploy "${SERVICE_NAME}" \
  --image="${IMAGE_URI}" \
  --platform=managed \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --memory="${MEMORY}" \
  --cpu="${CPU}" \
  --min-instances="${MIN_INSTANCES}" \
  --max-instances="${MAX_INSTANCES}" \
  --timeout=30 \
  --concurrency=80 \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},REGION=${REGION},ENDPOINT_ID=${ENDPOINT_ID},LOG_LEVEL=INFO" \
  --service-account="sensor-anomaly-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --allow-unauthenticated \
  --quiet

# ---------------------------------------------------------------------------
# 6. Print the service URL
# ---------------------------------------------------------------------------
echo ""
echo "[6/6] Fetching service URL …"
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --platform=managed \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(status.url)")

echo ""
echo "========================================="
echo "  Deployment complete!"
echo "  Service URL : ${SERVICE_URL}"
echo "  Health check: ${SERVICE_URL}/health"
echo "  Prediction  : ${SERVICE_URL}/predict"
echo "========================================="

# Quick smoke test
echo ""
echo "Running smoke test …"
curl -sf "${SERVICE_URL}/health" | python3 -m json.tool && echo "✅ Health check passed." \
  || echo "⚠️  Health check returned unexpected response."
