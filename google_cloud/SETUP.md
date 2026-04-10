# Google Cloud Setup Guide – Machine Sensor Anomaly Detection

This guide walks you through deploying the complete production-ready
sensor anomaly detection system on Google Cloud Platform.

---

## Architecture Overview

```
IoT Sensors
    │
    ▼
Cloud Pub/Sub  ──────────────────────────────────┐
    │                                             │
    ▼                                             │
Dataflow (Apache Beam)                            │
    │                                             │
    ▼                                             │
BigQuery ─────── Vertex AI Training Job           │
    │                   │                         │
    │            Vertex AI Endpoint               │
    │                   │                         │
    └──── Cloud Run API ◄─────────────────────────┘
                │
                ▼
       Cloud Monitoring
                │
                ▼
     Looker Studio / Data Studio
```

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python      | ≥ 3.11  |
| gcloud CLI  | latest  |
| Docker      | ≥ 24    |
| Terraform   | optional|

---

## Step 1 – Create a Google Cloud Project

```bash
# Create a new project (skip if reusing an existing one)
gcloud projects create sensor-anomaly-prod --name="Sensor Anomaly Detection"

# Set it as the active project
gcloud config set project sensor-anomaly-prod
export PROJECT_ID="sensor-anomaly-prod"
```

---

## Step 2 – Enable Required APIs

```bash
gcloud services enable \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  aiplatform.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  monitoring.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"
```

---

## Step 3 – Create a Service Account

```bash
SA_NAME="sensor-anomaly-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Create the service account
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="Sensor Anomaly Detection Service Account" \
  --project="${PROJECT_ID}"

# Grant required roles
for ROLE in \
  roles/pubsub.publisher \
  roles/pubsub.subscriber \
  roles/dataflow.worker \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser \
  roles/storage.objectAdmin \
  roles/aiplatform.user \
  roles/run.invoker \
  roles/monitoring.metricWriter
do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}" \
    --quiet
done

# Download key for local development (do NOT commit this file)
gcloud iam service-accounts keys create sa-key.json \
  --iam-account="${SA_EMAIL}" \
  --project="${PROJECT_ID}"

export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/sa-key.json"
```

> **Security note:** When running on GCP services (Cloud Run, Dataflow,
> Vertex AI), credential injection is handled automatically via the
> attached service account.  The `sa-key.json` file is only needed for
> local development and must be kept out of version control.

---

## Step 4 – Set Environment Variables

Create a `.env` file (excluded from git via `.dockerignore`):

```bash
cat > .env << 'EOF'
PROJECT_ID=sensor-anomaly-prod
REGION=us-central1
BUCKET=sensor-anomaly-prod-dataflow
BQ_DATASET=sensor_dataset
BQ_TABLE=raw_sensor_data
PUBSUB_TOPIC=sensor-data
PUBSUB_SUBSCRIPTION=sensor-data-sub
ENDPOINT_NAME=sensor-anomaly-endpoint
CONTAMINATION=0.05
LOOKBACK_DAYS=30
LOG_LEVEL=INFO
EOF

# Load into current shell
set -a && source .env && set +a
```

---

## Step 5 – Deploy Dataflow Pipeline

The deploy script creates all required resources and launches the
streaming pipeline:

```bash
bash dataflow_deploy.sh
```

Verify the job is running:

```bash
gcloud dataflow jobs list \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --filter="state=Running"
```

Monitor the pipeline in the Cloud Console:
`https://console.cloud.google.com/dataflow/jobs?project=YOUR_PROJECT_ID`

---

## Step 6 – Publish Test Sensor Data

```bash
# Install dependencies
pip install -r requirements.txt

# Publish 100 rounds of sensor readings (5 sensors × 100 rounds = 500 messages)
python pubsub_publisher.py \
  --project "${PROJECT_ID}" \
  --topic sensor-data \
  --sensors machine-001 machine-002 machine-003 \
  --anomaly-rate 0.05 \
  --interval 1.0
```

---

## Step 7 – Train the Vertex AI Model

After the Dataflow pipeline has ingested at least a few hundred rows, run
the training job:

```bash
# Local test run (writes artifact to a local directory)
export AIP_MODEL_DIR="./model_output"
python train_vertex_ai.py

# Submit as a Vertex AI Custom Training Job
gcloud ai custom-jobs create \
  --region="${REGION}" \
  --display-name="sensor-anomaly-training-$(date +%Y%m%d)" \
  --worker-pool-spec="machine-type=n1-standard-4,replica-count=1,container-image-uri=us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.1-3:latest" \
  --args="--project=${PROJECT_ID},--bq_dataset=${BQ_DATASET},--bq_table=${BQ_TABLE}" \
  --service-account="${SA_EMAIL}" \
  --project="${PROJECT_ID}"
```

---

## Step 8 – Deploy the Model to Vertex AI

Once training is complete and the artifact is in GCS:

```bash
python deploy_vertex_ai.py \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --model-dir "gs://${BUCKET}/models/" \
  --endpoint-name "${ENDPOINT_NAME}" \
  --image "us-central1-docker.pkg.dev/${PROJECT_ID}/sensor-anomaly-repo/predictor:latest"
```

Note the endpoint ID printed at the end – you will need it for Cloud Run:

```bash
export ENDPOINT_ID="<numeric-id-printed-above>"
```

---

## Step 9 – Deploy the Cloud Run API

```bash
bash cloudrun_deploy.sh
```

The script will print the service URL when complete.  Test it:

```bash
SERVICE_URL=$(gcloud run services describe sensor-anomaly-api \
  --region="${REGION}" --format="value(status.url)")

# Health check
curl "${SERVICE_URL}/health"

# Prediction
curl -X POST "${SERVICE_URL}/predict" \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"machine-001","temperature":25.5,"vibration":0.12,"pressure":1013.25}'
```

---

## Step 10 – Configure Monitoring & Alerts

### Create a notification channel

```bash
# Email channel
gcloud alpha monitoring channels create \
  --display-name="Sensor Anomaly Email Alerts" \
  --type=email \
  --channel-labels=email_address=your-email@example.com \
  --project="${PROJECT_ID}"

# Copy the channel ID from the output, then:
export CHANNEL_ID="<channel-id>"
```

### Deploy alert policies

Edit `alert_policy.yaml`, replacing `YOUR_PROJECT_ID` and
`YOUR_NOTIFICATION_CHANNEL_ID`, then:

```bash
# Deploy all three policies
gcloud alpha monitoring policies create \
  --policy-from-file=alert_policy.yaml \
  --project="${PROJECT_ID}"
```

Or use the Python helper:

```bash
python monitoring.py your-email@example.com
```

---

## Step 11 – Looker Studio Dashboard

1. Open [Looker Studio](https://datastudio.google.com/)
2. Click **Create → Report → Add data → BigQuery**
3. Select your project, dataset (`sensor_dataset`), and table
   (`raw_sensor_data`)
4. Use the SQL queries from `looker_dashboard.sql` as **Custom Query** data
   sources for each chart

Recommended chart types:
- **Query 1** → Time series chart (line)
- **Query 2** → Bar chart (anomaly count by hour)
- **Query 3** → Table (latest sensor status)
- **Query 4** → Scorecard (rolling anomaly rate)
- **Query 7** → Table (daily KPI summary)

---

## Teardown

To avoid unexpected charges, tear down resources when finished:

```bash
# Stop Dataflow pipeline
JOB_ID=$(gcloud dataflow jobs list \
  --region="${REGION}" --filter="name=${JOB_NAME} AND state=Running" \
  --format="value(id)" --limit=1)
gcloud dataflow jobs cancel "${JOB_ID}" --region="${REGION}"

# Delete Cloud Run service
gcloud run services delete sensor-anomaly-api \
  --region="${REGION}" --quiet

# Delete Vertex AI endpoint
gcloud ai endpoints delete "${ENDPOINT_ID}" \
  --region="${REGION}" --quiet

# Delete BigQuery dataset (caution: destroys data)
bq rm -r -f --dataset "${PROJECT_ID}:${BQ_DATASET}"

# Delete GCS bucket
gsutil rm -r "gs://${BUCKET}"

# Delete Pub/Sub subscription and topic
gcloud pubsub subscriptions delete sensor-data-sub
gcloud pubsub topics delete sensor-data
```

---

## Cost Estimate (Monthly)

| Service       | Usage                     | Estimated Cost |
|---------------|---------------------------|----------------|
| Pub/Sub       | 50 M messages             | ~$25           |
| Dataflow      | 4 vCPU × 8 h/day          | ~$100          |
| BigQuery      | 500 GB storage + queries  | ~$50           |
| Vertex AI     | Training 4×/month + serve | ~$100          |
| Cloud Run     | 1 M requests/month        | ~$25           |
| Cloud Monitoring | Custom metrics         | ~$10           |
| GCS           | 100 GB                    | ~$2            |
| **Total**     |                           | **~$312/month**|

> Costs vary by region and actual usage.  Enable [Budget Alerts](
> https://cloud.google.com/billing/docs/how-to/budgets) to avoid surprises.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `ENDPOINT_ID not set` | Env var missing | `export ENDPOINT_ID=<id>` |
| 403 on Vertex AI predict | IAM missing | Grant `aiplatform.user` to the Cloud Run SA |
| Dataflow job fails at startup | Missing GCS bucket | Run `dataflow_deploy.sh` first |
| No rows in BigQuery | Dataflow not running | Check Dataflow jobs in Cloud Console |
| High prediction latency | Cold start | Increase `--min-instances` in `cloudrun_deploy.sh` |
