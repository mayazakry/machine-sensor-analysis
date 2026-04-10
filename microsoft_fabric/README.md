# Microsoft Fabric – Machine Sensor Anomaly Detection

## Quick Start

> **No Azure charges until you run the deployment scripts.**
> All files in this folder are code/templates only.

### Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | ≥ 3.10 | [python.org](https://python.org) |
| Azure CLI | ≥ 2.55 | `winget install Microsoft.AzureCLI` |
| Docker | ≥ 24.0 | [docker.com](https://docker.com) |
| jq | any | `sudo apt install jq` |

### 1  Clone & install dependencies

```bash
cd microsoft_fabric
pip install -r requirements.txt
```

### 2  Configure environment

```bash
cp .env.example .env
# Edit .env with your Azure credentials and IDs
```

### 3  Train the model locally (FREE – no Azure)

```bash
python automl_trainer.py --backend sklearn --data ../data/sample_sensor_data.csv
```

### 4  Run the REST API locally (FREE)

```bash
python rest_api_app.py
# API available at http://localhost:8000
```

Test it:

```bash
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"machine_id":"M1","temperature":85,"vibration":0.5,"pressure":101,"humidity":50,"current":12,"voltage":220}' \
  | python -m json.tool
```

### 5  Deploy to Azure (charges apply)

```bash
# Set required environment variables first
export AZURE_SUBSCRIPTION_ID="<your-id>"
export AZURE_RESOURCE_GROUP="sensor-anomaly-rg"
export AZURE_REGION="eastus"
export ACR_NAME="sensoranomalyacr"
export FABRIC_WORKSPACE_ID="<your-workspace-guid>"
export FABRIC_LAKEHOUSE_ID="<your-lakehouse-guid>"

bash fabric_deploy.sh
```

## File Overview

| File | Purpose |
|------|---------|
| `event_hub_publisher.py` | Stream sensor readings to Azure Event Hub |
| `data_factory_pipeline.json` | ARM template: ingest → transform → load pipeline |
| `lakehouse_loader.py` | Write data to Fabric Lakehouse (Spark + REST) |
| `synapse_notebook.ipynb` | EDA, preprocessing & model training notebook |
| `automl_trainer.py` | Train Isolation Forest (local sklearn or Azure AutoML) |
| `rest_api_app.py` | Flask REST API for real-time predictions |
| `powerbi_queries.sql` | SQL + DAX queries for Power BI dashboard |
| `monitoring_alerts.py` | Azure Monitor metrics + Teams/email notifications |
| `fabric_deploy.sh` | Master deployment script |
| `event_hub_setup.sh` | Create Event Hub namespace and entity |
| `deploy_to_aci.sh` | Deploy REST API to Azure Container Instances |
| `powerbi_setup.sh` | Configure Power BI workspace and dataset |
| `requirements.txt` | Python dependencies |
| `config.yaml` | Configuration template |
| `.env.example` | Environment variable template |
| `Dockerfile` | Multi-stage container image for REST API |
| `SETUP.md` | Detailed step-by-step deployment guide |
| `ARCHITECTURE.md` | System architecture overview |

## Cost Estimate (Azure – USD/month)

| Component | SKU | Estimated Cost |
|-----------|-----|----------------|
| Event Hub Standard | 2 TU | ~$22 |
| Data Factory | 1000 pipeline runs | ~$5 |
| Azure Container Instances | 1 vCPU / 1.5 GB | ~$35 |
| OneLake Storage | 100 GB | ~$2 |
| Azure Monitor | Basic metrics | ~$5 |
| **Total** | | **~$69/month** |

> Free tier: new accounts receive $200 credits for 30 days.

## Support

See [SETUP.md](SETUP.md) for detailed instructions and troubleshooting.
See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design.
