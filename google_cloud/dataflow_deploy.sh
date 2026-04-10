#!/usr/bin/env bash
# =============================================================================
# dataflow_deploy.sh – Deploy the Apache Beam / Dataflow streaming pipeline
#
# This script creates the required GCS bucket, BigQuery dataset/table, and
# then launches the Dataflow streaming pipeline that reads from Pub/Sub and
# writes to BigQuery.
#
# Usage:
#   export PROJECT_ID=your-project-id
#   bash dataflow_deploy.sh
#
# Optional overrides:
#   REGION          – GCP region             (default: us-central1)
#   BUCKET          – GCS temp/staging bucket (default: ${PROJECT_ID}-dataflow)
#   BQ_DATASET      – BigQuery dataset        (default: sensor_dataset)
#   BQ_TABLE        – BigQuery table          (default: raw_sensor_data)
#   PUBSUB_TOPIC    – Pub/Sub topic name      (default: sensor-data)
#   JOB_NAME        – Dataflow job name       (default: sensor-anomaly-pipeline)
#   MAX_WORKERS     – Max Dataflow workers    (default: 10)
#   MACHINE_TYPE    – Worker machine type     (default: n1-standard-2)
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID environment variable must be set.}"
REGION="${REGION:-us-central1}"
BUCKET="${BUCKET:-${PROJECT_ID}-dataflow}"
BQ_DATASET="${BQ_DATASET:-sensor_dataset}"
BQ_TABLE="${BQ_TABLE:-raw_sensor_data}"
PUBSUB_TOPIC="${PUBSUB_TOPIC:-sensor-data}"
PUBSUB_SUBSCRIPTION="${PUBSUB_SUBSCRIPTION:-sensor-data-sub}"
JOB_NAME="${JOB_NAME:-sensor-anomaly-pipeline}"
MAX_WORKERS="${MAX_WORKERS:-10}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================="
echo "  Deploying Dataflow Pipeline"
echo "  Project    : ${PROJECT_ID}"
echo "  Region     : ${REGION}"
echo "  Bucket     : gs://${BUCKET}"
echo "  BQ table   : ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}"
echo "  Pub/Sub    : projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}"
echo "========================================="

# ---------------------------------------------------------------------------
# 1. Enable required APIs
# ---------------------------------------------------------------------------
echo ""
echo "[1/6] Enabling required APIs …"
gcloud services enable \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}" \
  --quiet

# ---------------------------------------------------------------------------
# 2. Create GCS bucket (idempotent)
# ---------------------------------------------------------------------------
echo ""
echo "[2/6] Ensuring GCS bucket exists: gs://${BUCKET} …"
gsutil ls -b "gs://${BUCKET}" 2>/dev/null \
  || gsutil mb -l "${REGION}" -p "${PROJECT_ID}" "gs://${BUCKET}"

# ---------------------------------------------------------------------------
# 3. Create BigQuery dataset and table (idempotent)
# ---------------------------------------------------------------------------
echo ""
echo "[3/6] Ensuring BigQuery dataset and table exist …"

bq --project_id="${PROJECT_ID}" ls "${BQ_DATASET}" 2>/dev/null \
  || bq --project_id="${PROJECT_ID}" mk \
       --dataset \
       --location="${REGION}" \
       "${BQ_DATASET}"

# Create table with schema if it does not exist
bq --project_id="${PROJECT_ID}" show "${BQ_DATASET}.${BQ_TABLE}" 2>/dev/null \
  || bq --project_id="${PROJECT_ID}" mk \
       --table \
       "${BQ_DATASET}.${BQ_TABLE}" \
       "sensor_id:STRING,timestamp:TIMESTAMP,temperature:FLOAT64,vibration:FLOAT64,pressure:FLOAT64,value:FLOAT64,is_anomaly:BOOL,anomaly_reason:STRING,processed_at:TIMESTAMP,pipeline_version:STRING"

# ---------------------------------------------------------------------------
# 4. Create Pub/Sub topic and subscription (idempotent)
# ---------------------------------------------------------------------------
echo ""
echo "[4/6] Ensuring Pub/Sub topic and subscription exist …"

gcloud pubsub topics describe "${PUBSUB_TOPIC}" \
  --project="${PROJECT_ID}" 2>/dev/null \
  || gcloud pubsub topics create "${PUBSUB_TOPIC}" \
       --project="${PROJECT_ID}"

gcloud pubsub subscriptions describe "${PUBSUB_SUBSCRIPTION}" \
  --project="${PROJECT_ID}" 2>/dev/null \
  || gcloud pubsub subscriptions create "${PUBSUB_SUBSCRIPTION}" \
       --topic="${PUBSUB_TOPIC}" \
       --ack-deadline=60 \
       --message-retention-duration=7d \
       --project="${PROJECT_ID}"

# ---------------------------------------------------------------------------
# 5. Install pipeline dependencies
# ---------------------------------------------------------------------------
echo ""
echo "[5/6] Installing Python dependencies …"
pip install --quiet -r "${SCRIPT_DIR}/requirements.txt"

# ---------------------------------------------------------------------------
# 6. Launch the Dataflow pipeline
# ---------------------------------------------------------------------------
echo ""
echo "[6/6] Launching Dataflow streaming pipeline …"
python "${SCRIPT_DIR}/dataflow_pipeline.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --temp_location="gs://${BUCKET}/temp" \
  --staging_location="gs://${BUCKET}/staging" \
  --job_name="${JOB_NAME}" \
  --max_num_workers="${MAX_WORKERS}" \
  --worker_machine_type="${MACHINE_TYPE}" \
  --subscription="projects/${PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION}" \
  --bq_table="${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}" \
  --streaming \
  --enable_streaming_engine

echo ""
echo "========================================="
echo "  Dataflow pipeline submitted!"
echo "  Monitor at:"
echo "  https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}"
echo "========================================="
