# Microsoft Fabric Setup Guide

Complete step-by-step instructions to deploy the machine sensor anomaly
detection system on Microsoft Azure and Microsoft Fabric.

> ⚠️ **Cost reminder**: Creating Azure resources incurs charges.
> All files in this folder are free to store and review.
> Charges start only when you execute the deployment scripts.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Create Azure Resources](#2-create-azure-resources)
3. [Set Up Event Hub](#3-set-up-event-hub)
4. [Configure Fabric Lakehouse](#4-configure-fabric-lakehouse)
5. [Deploy Data Factory Pipeline](#5-deploy-data-factory-pipeline)
6. [Train the Anomaly Detection Model](#6-train-the-anomaly-detection-model)
7. [Deploy the REST API](#7-deploy-the-rest-api)
8. [Create the Power BI Dashboard](#8-create-the-power-bi-dashboard)
9. [Set Up Monitoring and Alerts](#9-set-up-monitoring-and-alerts)
10. [Verify End-to-End Flow](#10-verify-end-to-end-flow)
11. [Cost Estimation](#11-cost-estimation)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Prerequisites

### 1.1 Accounts and Licences

- **Azure account** with an active subscription ([free trial](https://azure.microsoft.com/free/))
- **Microsoft Fabric** workspace (F64 SKU or Power BI Premium P1)
- **Power BI Pro** licence (for sharing dashboards)

### 1.2 Local Tools

```bash
# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az version   # should be ≥ 2.55

# Docker
sudo apt-get install docker.io -y
docker --version

# Python
python3 --version   # should be ≥ 3.10

# jq (for shell scripts)
sudo apt-get install jq -y

# Python dependencies
cd microsoft_fabric
pip install -r requirements.txt
```

### 1.3 Azure CLI Login

```bash
az login
az account set --subscription "YOUR_SUBSCRIPTION_ID"
az account show   # verify correct subscription
```

---

## 2. Create Azure Resources

### 2.1 Resource Group

```bash
export AZURE_RESOURCE_GROUP="sensor-anomaly-rg"
export AZURE_REGION="eastus"

az group create \
  --name     "$AZURE_RESOURCE_GROUP" \
  --location "$AZURE_REGION"
```

### 2.2 Azure Container Registry

```bash
export ACR_NAME="sensoranomalyacr"   # must be globally unique

az acr create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name           "$ACR_NAME" \
  --sku            Basic
```

---

## 3. Set Up Event Hub

### 3.1 Automated Setup

```bash
export EVENT_HUB_NAMESPACE="sensor-anomaly-eh"   # must be globally unique
export EVENT_HUB_NAME="sensor-events"

bash event_hub_setup.sh
```

This creates:
- Event Hub namespace (Standard SKU, 2 throughput units)
- `sensor-events` entity with 4 partitions, 7-day retention
- Consumer groups: `adf-consumer`, `app-consumer`
- SAS policies: `PublisherPolicy` (Send), `ConsumerPolicy` (Listen)

### 3.2 Save Connection String

```bash
# The script prints the connection strings.
# Add them to your .env file:
export EVENT_HUB_CONNECTION_STR="Endpoint=sb://..."
```

### 3.3 Fabric UI Alternative

1. Go to [Azure Portal](https://portal.azure.com) → **Event Hubs**
2. Click **+ Create** → fill in namespace name, region, SKU=Standard
3. Inside the namespace, click **+ Event Hub** → name: `sensor-events`
4. Go to **Shared Access Policies** → copy the connection string

---

## 4. Configure Fabric Lakehouse

### 4.1 Create Lakehouse (Fabric UI)

1. Go to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Select your workspace (or create one: **+ New workspace**)
3. Click **+ New** → **Lakehouse**
4. Name: `SensorLakehouse` → **Create**
5. Copy the **Workspace ID** and **Lakehouse ID** from the URL:
   ```
   https://app.fabric.microsoft.com/groups/<WORKSPACE_ID>/lakehouses/<LAKEHOUSE_ID>
   ```

### 4.2 Set Environment Variables

```bash
export FABRIC_WORKSPACE_ID="your-workspace-guid"
export FABRIC_LAKEHOUSE_ID="your-lakehouse-guid"
```

### 4.3 Create Delta Tables (SQL Endpoint)

In the Fabric Lakehouse SQL Endpoint, run:

```sql
-- Create anomaly predictions table
CREATE TABLE IF NOT EXISTS anomaly_predictions (
    machine_id      VARCHAR(50),
    timestamp       TIMESTAMP,
    is_anomaly      BOOLEAN,
    anomaly_score   DOUBLE,
    confidence      DOUBLE,
    features_used   STRING,
    event_date      DATE
)
USING DELTA
PARTITIONED BY (event_date);

-- Create processed sensor events table
CREATE TABLE IF NOT EXISTS processed_sensor_events (
    event_id              VARCHAR(100),
    machine_id            VARCHAR(50),
    timestamp             TIMESTAMP,
    temperature           DOUBLE,
    vibration             DOUBLE,
    pressure              DOUBLE,
    humidity              DOUBLE,
    current               DOUBLE,
    voltage               DOUBLE,
    temperature_zscore    DOUBLE,
    vibration_zscore      DOUBLE,
    pressure_zscore       DOUBLE,
    temperature_rolling_mean DOUBLE,
    vibration_rolling_mean   DOUBLE,
    hour_of_day           INT,
    day_of_week           INT,
    ingestion_timestamp   TIMESTAMP,
    event_date            DATE
)
USING DELTA
PARTITIONED BY (event_date);
```

---

## 5. Deploy Data Factory Pipeline

### 5.1 Automated Deployment

```bash
bash fabric_deploy.sh   # deploys ADF as part of full deployment
```

Or deploy ADF only:

```bash
EH_CONN_STR="$(az eventhubs namespace authorization-rule keys list \
  --resource-group   "$AZURE_RESOURCE_GROUP" \
  --namespace-name   "$EVENT_HUB_NAMESPACE" \
  --name             "RootManageSharedAccessKey" \
  --query            primaryConnectionString \
  --output           tsv)"

az deployment group create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --template-file  data_factory_pipeline.json \
  --parameters \
    factoryName="sensor-anomaly-adf" \
    eventHubConnectionString="$EH_CONN_STR" \
    eventHubName="sensor-events" \
    fabricWorkspaceId="$FABRIC_WORKSPACE_ID" \
    fabricLakehouseId="$FABRIC_LAKEHOUSE_ID"
```

### 5.2 Fabric UI Alternative

1. In Data Factory Studio, click **Author** → **+** → **Pipeline**
2. Add **Copy Data** activity → source: Event Hub, sink: Lakehouse Parquet
3. Add **Data Flow** activity for enrichment transformations
4. Set trigger: Recurrence every 1 hour
5. Publish all changes

### 5.3 Start the Trigger

```bash
az datafactory trigger start \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --factory-name   "sensor-anomaly-adf" \
  --name           "TR_Scheduled_Nightly"
```

---

## 6. Train the Anomaly Detection Model

### 6.1 Local Training (FREE – recommended first)

```bash
python automl_trainer.py \
  --backend sklearn \
  --data    data/sample_sensor_data.csv \
  --output-dir models/
```

> **Note**: The sample data file is located at `data/sample_sensor_data.csv` relative
> to the repository root. If running from the `microsoft_fabric/` directory, use
> `../data/sample_sensor_data.csv`, or provide the full path to your own sensor CSV.

Output:
```
models/isolation_forest_20260410T033434.pkl
models/model_latest.pkl
models/metadata_20260410T033434.json
```

### 6.2 Using the Synapse Notebook (inside Fabric)

1. In Microsoft Fabric, go to your workspace
2. Click **+ New** → **Notebook**
3. Import `synapse_notebook.ipynb`
4. Attach to a Spark pool (or use Starter Pool)
5. Set `RUN_LOCAL = False`
6. Run all cells (Shift+Enter or **Run All**)
7. Model is saved to `Lakehouse/Files/models/model_latest.pkl`

### 6.3 Azure AutoML (optional – charges apply)

```bash
python automl_trainer.py \
  --backend    azure \
  --data-asset sensor-data \
  --max-trials 20
```

---

## 7. Deploy the REST API

### 7.1 Test Locally First

```bash
# Ensure model is trained first
python rest_api_app.py

# Test endpoints
curl http://localhost:8000/health
curl http://localhost:8000/model/info
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"M1","temperature":150,"vibration":4.5,"pressure":90,"humidity":80,"current":25,"voltage":180}'
```

### 7.2 Build Docker Image

```bash
docker build -t sensor-api:latest .

# Test container locally
docker run -p 8000:8000 \
  -v $(pwd)/models:/app/models:ro \
  -e MODEL_PATH=/app/models/model_latest.pkl \
  sensor-api:latest

curl http://localhost:8000/health
```

### 7.3 Deploy to ACI

```bash
export ACR_NAME="sensoranomalyacr"
export ACI_NAME="sensor-api"

bash deploy_to_aci.sh
```

The script outputs the public API URL, e.g.:
```
http://sensor-api-sensor-anomaly-rg.eastus.azurecontainer.io:8000
```

---

## 8. Create the Power BI Dashboard

### 8.1 Automated Setup

```bash
export AZURE_TENANT_ID="your-tenant-id"
export POWERBI_CLIENT_ID="your-service-principal-id"
export POWERBI_CLIENT_SECRET="your-secret"

bash powerbi_setup.sh
```

### 8.2 Manual Setup in Power BI Desktop

1. Open **Power BI Desktop**
2. **Get Data** → **Microsoft Fabric (Lakehouse)**
3. Sign in and select `SensorLakehouse`
4. Select tables: `anomaly_predictions`, `processed_sensor_events`
5. Click **Transform Data** → paste queries from `powerbi_queries.sql`
6. Create visuals:
   - **Card** visual: `Anomaly Rate Last Hour %`
   - **Line chart**: `hour_bucket` (x) vs `anomaly_rate_pct` (y) by `machine_id`
   - **Bar chart**: top 10 anomalous machines
   - **Table**: latest 100 anomalies with severity
   - **Scatter chart**: vibration vs temperature coloured by `is_anomaly`
7. **Publish** to the `SensorAnomalyDashboard` workspace
8. **Pin** key visuals to a dashboard named `Sensor Anomaly Monitoring`

### 8.3 Add DAX Measures

In Power BI Desktop → **New Measure**, add the DAX from `powerbi_queries.sql`
Section 6 (copy each measure individually from the comment block).

---

## 9. Set Up Monitoring and Alerts

### 9.1 Teams Webhook

1. In Microsoft Teams, go to the target channel
2. Click **⋯** → **Connectors** → **Incoming Webhook** → **Configure**
3. Name: `Sensor Alerts`, upload an icon, click **Create**
4. Copy the webhook URL:
   ```bash
   export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/..."
   ```

### 9.2 Test Teams Notifications

```bash
python monitoring_alerts.py --mode test-teams
```

### 9.3 Start the Monitor

```bash
python monitoring_alerts.py --mode monitor
```

Or run it inside the ACI container by setting:
```bash
export REST_API_URL="http://your-aci-url:8000"
export ANOMALY_ALERT_THRESHOLD=0.10
```

### 9.4 Create Azure Monitor Alert Rules

```bash
python monitoring_alerts.py --mode create-alerts
```

---

## 10. Verify End-to-End Flow

```bash
# 1. Start publishing simulated sensor data
python event_hub_publisher.py --mode simulate --interval 0.5 --max-events 1000

# 2. Wait ~1 minute for ADF pipeline to ingest
# 3. Check Lakehouse tables
#    → Fabric Lakehouse SQL Endpoint → SELECT COUNT(*) FROM raw_sensor_events

# 4. Check REST API predictions
curl -X POST http://your-api-url/predict/batch \
  -H "Content-Type: application/json" \
  -d '{"readings": [
    {"machine_id":"M1","temperature":150,"vibration":4.5,"pressure":90,"humidity":80,"current":25,"voltage":180},
    {"machine_id":"M2","temperature":75,"vibration":0.5,"pressure":101,"humidity":50,"current":12,"voltage":220}
  ]}'

# 5. Refresh Power BI dashboard → anomalies should appear
# 6. Check Teams for alert notifications
```

---

## 11. Cost Estimation

| Component | SKU | Unit Price | Estimated Usage | Monthly Cost |
|-----------|-----|------------|-----------------|--------------|
| Event Hub | Standard 2 TU | $0.015/TU/hr | 2 TU × 720h | ~$22 |
| Event Hub ingress | | $0.028/million | 5M msgs/month | ~$0.14 |
| Data Factory | Data movement | $0.25/DIU·hr | 50 DIU·hrs | ~$12.50 |
| ACI | 1 vCPU | $0.0495/vCPU·hr | 1 vCPU × 720h | ~$35.64 |
| ACI | 1.5 GB RAM | $0.0055/GB·hr | 1.5 GB × 720h | ~$5.94 |
| ACR | Basic | $0.167/day | 30 days | ~$5 |
| OneLake Storage | | $0.023/GB/month | 100 GB | ~$2.30 |
| Azure Monitor | Custom metrics | $0.258/metric/month | 10 metrics | ~$2.58 |
| ACS Email | | $0.0025/email | 100 emails | ~$0.25 |
| **Total** | | | | **~$86/month** |

> 💡 **Cost saving tips**:
> - Use **Consumption plan** ACI (billed per second, not hour)
> - Set ADF trigger to every 4 hours instead of 1 hour
> - Use Basic Event Hub SKU for development
> - New accounts receive **$200 free credits** for 30 days

---

## 12. Troubleshooting

### Event Hub connection fails

```bash
# Verify connection string
az eventhubs namespace authorization-rule keys list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --namespace-name "$EVENT_HUB_NAMESPACE" \
  --name           "RootManageSharedAccessKey"
```

### ADF pipeline fails

1. Go to Azure Data Factory Studio → **Monitor**
2. Click the failed run → check activity error details
3. Common causes: incorrect Lakehouse path, missing permissions

### REST API returns 503

```bash
# Check model exists
ls -la models/model_latest.pkl

# Train model first
python automl_trainer.py --backend sklearn --data ../data/sample_sensor_data.csv
```

### ACI container not starting

```bash
az container logs \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name           "$ACI_NAME"
```

### Power BI dataset not refreshing

1. In Power BI Service → Dataset → **Settings** → **Data source credentials**
2. Re-enter Lakehouse credentials
3. Check gateway is online (if using on-premises gateway)

### Teams webhook not working

```bash
# Test manually
curl -X POST "$TEAMS_WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d '{"text":"Test alert from sensor system"}'
# Expected: HTTP 200 with body "1"
```
