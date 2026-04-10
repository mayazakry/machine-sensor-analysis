# System Architecture – Machine Sensor Anomaly Detection on Microsoft Fabric

## Overview

This system ingests machine sensor telemetry in real time, detects anomalies
using a trained Isolation Forest model, and visualises results in Power BI –
all running on Microsoft Fabric and Azure services.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        IoT / Sensor Layer                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │ Machine 1│  │ Machine 2│  │ Machine N│  │ Sensor Simulator │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘   │
│       └─────────────┴─────────────┴─────────────────┘             │
│                              │  JSON over HTTPS / AMQP              │
└──────────────────────────────┼──────────────────────────────────────┘
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Ingestion Layer – Azure Event Hub                 │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │  Event Hub Namespace: sensor-anomaly-eh                    │      │
│  │  Entity: sensor-events  │  Partitions: 4  │  Retention: 7d│      │
│  │  Consumer Groups: adf-consumer, app-consumer               │      │
│  └────────────────────────────────────────────────────────────┘      │
└──────────────────────┬────────────────────────────────────────────────┘
                       │  Trigger every 1 hour (ADF schedule)
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                 ETL Layer – Azure Data Factory                       │
│  ┌─────────────────────────────────────────────────────────┐         │
│  │  PL_SensorAnomalyDetectionFull (master pipeline)         │         │
│  │  ├── PL_IngestSensorEvents  (Copy: EventHub → Parquet)   │         │
│  │  └── PL_TransformSensorData (DataFlow: enrich + z-score) │         │
│  └─────────────────────────────────────────────────────────┘         │
└──────────────────────┬────────────────────────────────────────────────┘
                       │  Write partitioned Parquet files
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│              Storage Layer – Microsoft Fabric Lakehouse              │
│  ┌───────────────────────────────────────────────────────────┐        │
│  │  OneLake / SensorLakehouse                                │        │
│  │  Tables/                                                  │        │
│  │  ├── raw_sensor_events/        (Delta, partitioned by date)│        │
│  │  ├── processed_sensor_events/  (Delta, enriched features) │        │
│  │  └── anomaly_predictions/      (Delta, model outputs)     │        │
│  │  Files/                                                   │        │
│  │  ├── models/                   (pickled artefacts)        │        │
│  │  └── raw_sensor_events/YYYY/MM/DD/*.parquet               │        │
│  └───────────────────────────────────────────────────────────┘        │
└──────────────────────┬────────────────────────────────────────────────┘
                       │  PySpark read / REST API reads model
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│               Analytics Layer – Synapse / Fabric Notebooks           │
│  ┌────────────────────────────────────────────────────────────┐       │
│  │  synapse_notebook.ipynb                                    │       │
│  │  1. Load Delta tables → pandas DataFrame                   │       │
│  │  2. EDA: distributions, correlations, time trends          │       │
│  │  3. Feature engineering & scaling                          │       │
│  │  4. Train Isolation Forest (grid search)                   │       │
│  │  5. Evaluate: F1, ROC AUC, confusion matrix                │       │
│  │  6. Save model_latest.pkl → Lakehouse Files                │       │
│  └────────────────────────────────────────────────────────────┘       │
└──────────────────────┬────────────────────────────────────────────────┘
                       │  model_latest.pkl mounted into container
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│              Serving Layer – REST API on Azure Container Instances   │
│  ┌─────────────────────────────────────────────────────────────┐      │
│  │  Flask (gunicorn) – rest_api_app.py                         │      │
│  │  GET  /health                  ← ACI health probe            │      │
│  │  GET  /model/info              ← model metadata              │      │
│  │  POST /predict                 ← single reading prediction   │      │
│  │  POST /predict/batch           ← bulk prediction (≤ 1000)    │      │
│  │  GET  /metrics                 ← operational counters        │      │
│  └─────────────────────────────────────────────────────────────┘      │
└──────────────────────┬────────────────────────────────────────────────┘
                       │  Write predictions + anomaly scores
                       ▼ (via lakehouse_loader.py)
           ┌──────────────────────────────┐
           │   anomaly_predictions (Delta) │
           └──────────────┬───────────────┘
                          │  DirectLake / Import
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│             Visualisation Layer – Power BI                           │
│  ┌────────────────────────────────────────────────────────────┐       │
│  │  Real-time Dashboard                                       │       │
│  │  ├── Anomaly Rate KPI card                                 │       │
│  │  ├── Hourly anomaly trend line chart                       │       │
│  │  ├── Machine comparison bar chart                          │       │
│  │  ├── Sensor distribution histograms                        │       │
│  │  ├── Live alert feed table                                 │       │
│  │  └── Confusion matrix / ROC visual (model performance)     │       │
│  └────────────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
                          │  Above threshold → alert
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│              Monitoring Layer – Azure Monitor + Teams                │
│  ┌────────────────────────────────────────────────────────────┐       │
│  │  monitoring_alerts.py                                      │       │
│  │  ├── AzureMonitorPublisher – custom metric ingestion        │       │
│  │  ├── TeamsNotifier          – adaptive card notifications   │       │
│  │  ├── EmailAlerter           – Azure Communication Services  │       │
│  │  └── AlertRuleManager       – programmatic alert rules      │       │
│  └────────────────────────────────────────────────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow (Detailed)

```
1. Sensor Reading Generated
   event_hub_publisher.py → JSON payload
   {
     "event_id": "MACHINE_001_1712720074000",
     "machine_id": "MACHINE_001",
     "timestamp": "2026-04-10T03:34:34+00:00",
     "temperature": 82.45,
     "vibration": 0.52,
     "pressure": 101.8,
     "humidity": 47.3,
     "current": 11.9,
     "voltage": 219.5
   }

2. Event Hub receives → stores for up to 7 days
   Partitioned by machine_id hash

3. ADF pipeline runs hourly
   Copy activity → Parquet files in Lakehouse/Files/raw/YYYY/MM/DD/
   DataFlow → adds rolling_mean, rolling_std, zscore columns
   Output → Lakehouse/Tables/processed_sensor_events (Delta)

4. REST API receives POST /predict
   → loads model_latest.pkl
   → StandardScaler.transform(features)
   → IsolationForest.predict() → 1 (normal) or -1 (anomaly)
   → IsolationForest.score_samples() → anomaly score
   → returns {"is_anomaly": true, "anomaly_score": 0.82, ...}

5. Predictions written to anomaly_predictions Delta table

6. Power BI DirectLake reads anomaly_predictions
   → refreshes dashboards in near real-time

7. monitoring_alerts.py polls /metrics every 60s
   → if anomaly_rate > 10% → Teams card + email sent
   → Azure Monitor custom metric ingested
```

---

## Technology Choices

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Message bus | Azure Event Hub | High-throughput, native ADF integration |
| ETL | Azure Data Factory | Low-code, built-in Event Hub connector |
| Storage | Fabric Lakehouse (Delta) | ACID, time-travel, Power BI DirectLake |
| ML | Isolation Forest | Unsupervised, no labels needed, interpretable |
| API | Flask + gunicorn | Lightweight, well-understood, easy to containerise |
| Container host | Azure Container Instances | Serverless containers, lowest operational overhead |
| BI | Power BI | Native Fabric integration, DirectLake mode |
| Alerts | Teams Incoming Webhook | Zero-infrastructure, instant notification |

---

## Security Considerations

- **Secrets** stored in Azure Key Vault; never in source code.
- **Managed Identity** used for ADF → Lakehouse and ACI → ACR access.
- **API Key** header (`X-API-Key`) for REST API authentication.
- **HTTPS** enforced by ACI DNS / Application Gateway (not included in scripts).
- **Role-Based Access Control (RBAC)** applied to all Azure resources.
- **Non-root container** user (`appuser:appgroup`) in Dockerfile.
- **Network Security Group** restricts ACI port to known IPs.

---

## Scalability

| Bottleneck | Solution |
|------------|----------|
| Event Hub throughput | Increase Throughput Units (auto-inflate enabled) |
| ADF processing | Increase DataFlow cluster cores |
| REST API latency | Scale out ACI to multiple instances behind a load balancer |
| Lakehouse storage | OneLake scales automatically |
| Power BI | Premium capacity for larger audiences |
