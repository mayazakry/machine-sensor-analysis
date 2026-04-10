"""
Vertex AI Training Script – Isolation Forest on BigQuery Sensor Data.

This script is designed to run as a **custom training job** on Vertex AI.
It reads historical sensor readings from BigQuery, trains an Isolation Forest
anomaly-detection model, evaluates it, and saves the artifact to Google Cloud
Storage in a format compatible with Vertex AI Model Registry.

Vertex AI will inject the following environment variables automatically when
the job is submitted via the Vertex AI SDK or ``gcloud``:

    AIP_MODEL_DIR   – GCS URI where the model artifact must be written
                      (e.g. ``gs://bucket/model/``).

Additional environment variables (set via CustomTrainingJob.run or the
training script's own args):

    PROJECT_ID      – GCP project ID
    BQ_DATASET      – BigQuery dataset name (default: sensor_dataset)
    BQ_TABLE        – BigQuery table name  (default: raw_sensor_data)
    CONTAMINATION   – Expected fraction of anomalies (default: 0.05)
    LOOKBACK_DAYS   – Days of data to include in training (default: 30)

Usage (local test):
    export AIP_MODEL_DIR=gs://your-bucket/models/
    export PROJECT_ID=your-project-id
    python train_vertex_ai.py
"""

import json
import logging
import os
import sys
import tempfile

import joblib
import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (from environment variables)
# ---------------------------------------------------------------------------
MODEL_DIR = os.environ.get("AIP_MODEL_DIR", "gs://your-bucket/models/")
PROJECT_ID = os.environ.get("PROJECT_ID", "your-project-id")
BQ_DATASET = os.environ.get("BQ_DATASET", "sensor_dataset")
BQ_TABLE = os.environ.get("BQ_TABLE", "raw_sensor_data")
CONTAMINATION = float(os.environ.get("CONTAMINATION", "0.05"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))

# Feature columns used for training
FEATURE_COLUMNS = ["temperature", "vibration", "pressure", "value"]
# Target label (1 = normal, -1 = anomaly) – derived from threshold flagging
LABEL_COLUMN = "is_anomaly"

# ---------------------------------------------------------------------------
# BigQuery data loading
# ---------------------------------------------------------------------------


def load_training_data(project_id: str, dataset: str, table: str, lookback_days: int) -> pd.DataFrame:
    """Load the last *lookback_days* days of sensor data from BigQuery.

    Args:
        project_id: GCP project ID.
        dataset: BigQuery dataset name.
        table: BigQuery table name.
        lookback_days: Number of calendar days to include (from now).

    Returns:
        A :class:`pandas.DataFrame` with columns for each feature and the
        ``is_anomaly`` label.

    Raises:
        RuntimeError: If the query returns no rows.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT
            temperature,
            vibration,
            pressure,
            value,
            CAST(is_anomaly AS INT64) AS is_anomaly_int
        FROM
            `{project_id}.{dataset}.{table}`
        WHERE
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), DAY) <= {lookback_days}
            AND temperature IS NOT NULL
            AND vibration IS NOT NULL
            AND pressure IS NOT NULL
            AND value IS NOT NULL
        ORDER BY
            timestamp DESC
    """
    logger.info("Querying BigQuery: %s.%s.%s (last %d days)", project_id, dataset, table, lookback_days)
    df = client.query(query).to_dataframe()

    if df.empty:
        raise RuntimeError(
            f"No training data found in {project_id}.{dataset}.{table} "
            f"for the last {lookback_days} days."
        )

    logger.info("Loaded %d rows from BigQuery.", len(df))
    return df


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------


def train_model(df: pd.DataFrame, contamination: float):
    """Train an Isolation Forest and a feature scaler on *df*.

    Args:
        df: DataFrame containing FEATURE_COLUMNS.
        contamination: Expected fraction of outliers in the training data.

    Returns:
        Tuple of (fitted IsolationForest, fitted StandardScaler).
    """
    X = df[FEATURE_COLUMNS].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info(
        "Training IsolationForest | contamination=%.3f | n_samples=%d | n_features=%d",
        contamination, X.shape[0], X.shape[1],
    )

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        max_features=len(FEATURE_COLUMNS),
        bootstrap=True,
        n_jobs=-1,
        random_state=42,
        verbose=0,
    )
    model.fit(X_scaled)

    logger.info("Model training complete.")
    return model, scaler


# ---------------------------------------------------------------------------
# Model evaluation
# ---------------------------------------------------------------------------


def evaluate_model(model: IsolationForest, scaler: StandardScaler, df: pd.DataFrame) -> dict:
    """Evaluate model quality against the threshold-based labels in *df*.

    Isolation Forest is an unsupervised algorithm, so we use the
    ``is_anomaly`` column (derived from physical thresholds by the Dataflow
    pipeline) as a weak label for reference only.

    Args:
        model: Fitted :class:`IsolationForest`.
        scaler: Fitted :class:`StandardScaler`.
        df: DataFrame with feature columns and an ``is_anomaly_int`` column
            (0 = normal, 1 = anomaly).

    Returns:
        Dictionary of evaluation metrics.
    """
    X = scaler.transform(df[FEATURE_COLUMNS].values)
    raw_preds = model.predict(X)  # 1 = normal, -1 = anomaly
    y_pred = (raw_preds == -1).astype(int)  # 1 = anomaly, 0 = normal

    y_true = df["is_anomaly_int"].values if "is_anomaly_int" in df.columns else np.zeros(len(y_pred))

    report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
    cm = confusion_matrix(y_true, y_pred).tolist()

    anomaly_scores = -model.score_samples(X)  # Higher = more anomalous
    metrics = {
        "n_samples": len(y_true),
        "n_anomalies_predicted": int(y_pred.sum()),
        "anomaly_fraction_predicted": float(y_pred.mean()),
        "mean_anomaly_score": float(anomaly_scores.mean()),
        "std_anomaly_score": float(anomaly_scores.std()),
        "classification_report": report,
        "confusion_matrix": cm,
    }

    logger.info("Evaluation metrics: %s", json.dumps(
        {k: v for k, v in metrics.items() if k not in ("classification_report", "confusion_matrix")},
        indent=2,
    ))
    return metrics


# ---------------------------------------------------------------------------
# Artifact saving to GCS
# ---------------------------------------------------------------------------


def save_artifacts(
    model: IsolationForest,
    scaler: StandardScaler,
    metrics: dict,
    model_dir: str,
) -> None:
    """Save the model, scaler, and metrics to *model_dir* on GCS.

    The directory layout expected by Vertex AI custom prediction routines is::

        model_dir/
            model.joblib    – IsolationForest
            scaler.joblib   – StandardScaler
            metrics.json    – Evaluation metrics

    Args:
        model: Fitted IsolationForest.
        scaler: Fitted StandardScaler.
        metrics: Dictionary of evaluation metrics.
        model_dir: GCS URI (``gs://bucket/path/``) or local path.
    """
    if model_dir.startswith("gs://"):
        _save_to_gcs(model, scaler, metrics, model_dir)
    else:
        _save_locally(model, scaler, metrics, model_dir)


def _save_to_gcs(model, scaler, metrics, gcs_dir: str) -> None:
    """Internal helper that serialises artifacts and uploads them to GCS."""
    # Parse bucket / blob prefix from the GCS URI
    without_scheme = gcs_dir[len("gs://"):]
    bucket_name, _, prefix = without_scheme.partition("/")
    prefix = prefix.rstrip("/")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    with tempfile.TemporaryDirectory() as tmpdir:
        model_local = os.path.join(tmpdir, "model.joblib")
        scaler_local = os.path.join(tmpdir, "scaler.joblib")
        metrics_local = os.path.join(tmpdir, "metrics.json")

        joblib.dump(model, model_local)
        joblib.dump(scaler, scaler_local)
        with open(metrics_local, "w") as fh:
            json.dump(metrics, fh, indent=2, default=str)

        for filename in ("model.joblib", "scaler.joblib", "metrics.json"):
            blob_name = f"{prefix}/{filename}" if prefix else filename
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(os.path.join(tmpdir, filename))
            logger.info("Uploaded gs://%s/%s", bucket_name, blob_name)


def _save_locally(model, scaler, metrics, local_dir: str) -> None:
    """Internal helper that serialises artifacts to *local_dir*."""
    os.makedirs(local_dir, exist_ok=True)
    joblib.dump(model, os.path.join(local_dir, "model.joblib"))
    joblib.dump(scaler, os.path.join(local_dir, "scaler.joblib"))
    with open(os.path.join(local_dir, "metrics.json"), "w") as fh:
        json.dump(metrics, fh, indent=2, default=str)
    logger.info("Artifacts saved to %s", local_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logger.info("=== Vertex AI Training Job Starting ===")
    logger.info("Project    : %s", PROJECT_ID)
    logger.info("BQ table   : %s.%s.%s", PROJECT_ID, BQ_DATASET, BQ_TABLE)
    logger.info("Model dir  : %s", MODEL_DIR)
    logger.info("Lookback   : %d days", LOOKBACK_DAYS)
    logger.info("Contamination : %.3f", CONTAMINATION)

    # 1. Load data
    df = load_training_data(PROJECT_ID, BQ_DATASET, BQ_TABLE, LOOKBACK_DAYS)

    # 2. Train/test split for evaluation (training uses all data)
    train_df, eval_df = train_test_split(df, test_size=0.2, random_state=42)
    logger.info("Train / eval split: %d / %d", len(train_df), len(eval_df))

    # 3. Train model on full dataset
    model, scaler = train_model(df, contamination=CONTAMINATION)

    # 4. Evaluate on held-out set
    metrics = evaluate_model(model, scaler, eval_df)

    # 5. Save artifacts to GCS (or local path for unit tests)
    save_artifacts(model, scaler, metrics, MODEL_DIR)

    logger.info("=== Training Job Complete ===")


if __name__ == "__main__":
    main()
