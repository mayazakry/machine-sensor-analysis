"""
AutoML Model Trainer - Train Isolation Forest anomaly detection model.

This module supports two training backends:
  1. **Sklearn** – local/ACI training with full hyperparameter tuning via
     scikit-learn and MLflow tracking.
  2. **Azure AutoML** – submit a training run to Azure Machine Learning /
     Fabric AutoML and retrieve the best model.

The trained model artefact is saved to:
  - A local ``.pkl`` file for direct loading.
  - An MLflow run (if MLflow tracking URI is configured).
  - Azure ML Model Registry (Azure AutoML path).

Usage (local sklearn):
    python automl_trainer.py --backend sklearn --data data/sample_sensor_data.csv

Usage (Azure AutoML):
    python automl_trainer.py --backend azure --data data/sample_sensor_data.csv
"""

import os
import json
import pickle
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import ParameterGrid
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_OUTPUT_DIR = os.getenv("MODEL_OUTPUT_DIR", "models")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "sensor-anomaly-detection")
AML_SUBSCRIPTION_ID = os.getenv("AML_SUBSCRIPTION_ID", "")
AML_RESOURCE_GROUP = os.getenv("AML_RESOURCE_GROUP", "")
AML_WORKSPACE_NAME = os.getenv("AML_WORKSPACE_NAME", "")
AML_COMPUTE_CLUSTER = os.getenv("AML_COMPUTE_CLUSTER", "cpu-cluster")

# Sensor feature columns used for training
FEATURE_COLUMNS = [
    "temperature",
    "vibration",
    "pressure",
    "humidity",
    "current",
    "voltage",
]
LABEL_COLUMN = "is_simulated_anomaly"  # Optional ground-truth label for evaluation

# Hyperparameter search space
PARAM_GRID = {
    "n_estimators": [100, 200, 300],
    "max_samples": ["auto", 0.8, 0.6],
    "contamination": [0.03, 0.05, 0.07, 0.10],
    "max_features": [0.8, 1.0],
    "random_state": [42],
}


# ---------------------------------------------------------------------------
# Data Preparation
# ---------------------------------------------------------------------------
def load_and_prepare(
    csv_path: str,
    feature_cols: Optional[list] = None,
) -> Tuple[np.ndarray, Optional[np.ndarray], StandardScaler]:
    """Load CSV data and scale features.

    Args:
        csv_path: Path to the sensor data CSV.
        feature_cols: List of feature column names. Defaults to FEATURE_COLUMNS.

    Returns:
        Tuple of (scaled feature matrix, optional label array, fitted scaler).
    """
    feature_cols = feature_cols or FEATURE_COLUMNS
    df = pd.read_csv(csv_path)
    logger.info("Loaded %d rows from '%s'", len(df), csv_path)

    # Keep only available feature columns
    available = [c for c in feature_cols if c in df.columns]
    if not available:
        raise ValueError(
            f"None of the expected feature columns found. "
            f"Expected: {feature_cols}, Got: {df.columns.tolist()}"
        )
    logger.info("Training features: %s", available)

    X = df[available].fillna(df[available].mean()).values
    y: Optional[np.ndarray] = None
    if LABEL_COLUMN in df.columns:
        y = df[LABEL_COLUMN].astype(int).values
        logger.info(
            "Labels present – anomaly rate: %.2f%%", 100 * y.mean()
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, y, scaler


# ---------------------------------------------------------------------------
# Sklearn Trainer
# ---------------------------------------------------------------------------
class SklearnAnomalyTrainer:
    """Train and evaluate Isolation Forest with optional MLflow tracking.

    Performs a grid search over the configured hyperparameter space and
    selects the model with the lowest reconstruction score (best anomaly
    separation) if no labels are available, or the best F1 score if labels
    are provided.
    """

    def __init__(
        self,
        param_grid: Optional[Dict[str, Any]] = None,
        output_dir: str = MODEL_OUTPUT_DIR,
    ) -> None:
        self.param_grid = param_grid or PARAM_GRID
        self.output_dir = output_dir
        self.best_model: Optional[IsolationForest] = None
        self.best_params: Dict[str, Any] = {}
        self.best_score: float = -np.inf
        self.scaler: Optional[StandardScaler] = None
        os.makedirs(output_dir, exist_ok=True)

    def _try_mlflow(self, run_name: str):
        """Context manager wrapper for optional MLflow run."""
        try:
            import mlflow  # noqa: PLC0415

            if MLFLOW_TRACKING_URI:
                mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.set_experiment(EXPERIMENT_NAME)
            return mlflow.start_run(run_name=run_name)
        except ImportError:
            logger.warning("mlflow not installed – skipping tracking.")
            return _NullContext()

    def train(
        self,
        X: np.ndarray,
        y: Optional[np.ndarray] = None,
    ) -> IsolationForest:
        """Run grid search and return the best Isolation Forest model.

        Args:
            X: Scaled feature matrix (n_samples, n_features).
            y: Optional ground-truth binary label array (1=anomaly, 0=normal).

        Returns:
            Best-fitting IsolationForest model.
        """
        logger.info(
            "Starting grid search over %d parameter combinations",
            len(list(ParameterGrid(self.param_grid))),
        )
        grid = list(ParameterGrid(self.param_grid))

        for params in grid:
            model = IsolationForest(**params)
            preds_raw = model.fit_predict(X)  # 1=normal, -1=anomaly
            preds_binary = (preds_raw == -1).astype(int)

            if y is not None:
                score = f1_score(y, preds_binary, zero_division=0)
            else:
                # Use mean anomaly score (higher negative score = more anomalous separation)
                scores = model.score_samples(X)
                score = float(-np.std(scores))  # higher spread → better separation

            if score > self.best_score:
                self.best_score = score
                self.best_model = model
                self.best_params = params

        metric_name = "f1_score" if y is not None else "score_std"
        logger.info(
            "Best params: %s | %s=%.4f", self.best_params, metric_name, self.best_score
        )
        return self.best_model  # type: ignore[return-value]

    def evaluate(
        self,
        model: IsolationForest,
        X: np.ndarray,
        y: np.ndarray,
    ) -> Dict[str, float]:
        """Evaluate the model against labelled data.

        Args:
            model: Trained IsolationForest.
            X: Feature matrix.
            y: Ground-truth binary labels (1=anomaly).

        Returns:
            Dictionary of evaluation metrics.
        """
        preds_raw = model.predict(X)
        preds_binary = (preds_raw == -1).astype(int)
        scores = model.score_samples(X)

        metrics = {
            "precision": float(precision_score(y, preds_binary, zero_division=0)),
            "recall": float(recall_score(y, preds_binary, zero_division=0)),
            "f1": float(f1_score(y, preds_binary, zero_division=0)),
        }
        try:
            metrics["roc_auc"] = float(roc_auc_score(y, -scores))
        except ValueError:
            metrics["roc_auc"] = 0.0

        logger.info("Evaluation metrics: %s", metrics)
        return metrics

    def save(
        self,
        model: IsolationForest,
        scaler: StandardScaler,
        metrics: Optional[Dict[str, float]] = None,
    ) -> str:
        """Persist model artefacts to disk.

        Args:
            model: Trained model.
            scaler: Fitted feature scaler.
            metrics: Optional evaluation metrics to embed in metadata.

        Returns:
            Path to the saved model file.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        artefact = {
            "model": model,
            "scaler": scaler,
            "params": self.best_params,
            "metrics": metrics or {},
            "trained_at": timestamp,
            "feature_columns": FEATURE_COLUMNS,
        }
        path = os.path.join(self.output_dir, f"isolation_forest_{timestamp}.pkl")
        with open(path, "wb") as fh:
            pickle.dump(artefact, fh)

        # Also write a stable "latest" symlink
        latest_path = os.path.join(self.output_dir, "model_latest.pkl")
        with open(latest_path, "wb") as fh:
            pickle.dump(artefact, fh)

        # Save metadata sidecar
        meta_path = os.path.join(self.output_dir, f"metadata_{timestamp}.json")
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "params": self.best_params,
                    "metrics": metrics or {},
                    "trained_at": timestamp,
                    "feature_columns": FEATURE_COLUMNS,
                },
                fh,
                indent=2,
            )

        logger.info("Model saved to '%s'", path)
        return path

    def run(self, csv_path: str) -> str:
        """End-to-end training pipeline.

        Args:
            csv_path: Path to sensor data CSV.

        Returns:
            Path to the saved model file.
        """
        X, y, scaler = load_and_prepare(csv_path)
        self.scaler = scaler

        with self._try_mlflow("isolation_forest_grid_search"):
            model = self.train(X, y)
            metrics: Optional[Dict[str, float]] = None
            if y is not None:
                metrics = self.evaluate(model, X, y)
                try:
                    import mlflow  # noqa: PLC0415

                    mlflow.log_params(self.best_params)
                    mlflow.log_metrics(metrics)
                except Exception:  # noqa: BLE001
                    pass

        return self.save(model, scaler, metrics)


# ---------------------------------------------------------------------------
# Azure AutoML Trainer
# ---------------------------------------------------------------------------
class AzureAutoMLTrainer:
    """Submit an anomaly detection job to Azure Machine Learning AutoML.

    Requires the ``azure-ai-ml`` SDK and a configured AML workspace.
    """

    def __init__(
        self,
        subscription_id: str = AML_SUBSCRIPTION_ID,
        resource_group: str = AML_RESOURCE_GROUP,
        workspace_name: str = AML_WORKSPACE_NAME,
        compute_cluster: str = AML_COMPUTE_CLUSTER,
    ) -> None:
        """Initialise the Azure AutoML trainer.

        Args:
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group name.
            workspace_name: AML workspace name.
            compute_cluster: AML compute cluster name.
        """
        try:
            from azure.ai.ml import MLClient  # noqa: PLC0415
            from azure.identity import DefaultAzureCredential  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "azure-ai-ml required. Run: pip install azure-ai-ml azure-identity"
            )

        credential = DefaultAzureCredential()
        self.client = MLClient(  # type: ignore[name-defined]
            credential=credential,
            subscription_id=subscription_id,
            resource_group_name=resource_group,
            workspace_name=workspace_name,
        )
        self.compute_cluster = compute_cluster
        logger.info("AzureAutoMLTrainer connected to workspace '%s'", workspace_name)

    def submit_job(
        self,
        data_asset_name: str,
        experiment_name: str = EXPERIMENT_NAME,
        max_trials: int = 20,
        timeout_minutes: int = 60,
    ) -> str:
        """Submit an AutoML classification/anomaly detection job.

        Because AutoML does not natively support unsupervised anomaly
        detection, we frame the problem as binary classification where the
        ``is_anomaly`` column is the target.

        Args:
            data_asset_name: Name of the registered AML data asset.
            experiment_name: AML experiment name.
            max_trials: Maximum number of AutoML trials.
            timeout_minutes: Job timeout.

        Returns:
            AML job name.
        """
        from azure.ai.ml import automl, Input  # noqa: PLC0415
        from azure.ai.ml.constants import AssetTypes  # noqa: PLC0415

        training_data = Input(
            type=AssetTypes.MLTABLE,
            path=f"azureml:{data_asset_name}:latest",
        )

        classification_job = automl.classification(
            compute=self.compute_cluster,
            experiment_name=experiment_name,
            training_data=training_data,
            target_column_name="is_anomaly",
            primary_metric="f1_score_weighted",
            n_cross_validations=5,
            enable_model_explainability=True,
        )
        classification_job.set_limits(
            timeout_minutes=timeout_minutes,
            trial_timeout_minutes=15,
            max_trials=max_trials,
        )

        job = self.client.jobs.create_or_update(classification_job)
        logger.info("AutoML job submitted: %s", job.name)
        return job.name

    def register_best_model(self, job_name: str, model_name: str = "sensor-anomaly-model") -> None:
        """Register the best AutoML model to the AML Model Registry.

        Args:
            job_name: Completed AutoML job name.
            model_name: Name to register the model under.
        """
        from azure.ai.ml.entities import Model  # noqa: PLC0415
        from azure.ai.ml.constants import AssetTypes  # noqa: PLC0415

        model = Model(
            path=f"azureml://jobs/{job_name}/outputs/artifacts/outputs/model.pkl",
            name=model_name,
            description="Sensor anomaly detection model trained via AutoML",
            type=AssetTypes.CUSTOM_MODEL,
        )
        registered = self.client.models.create_or_update(model)
        logger.info(
            "Model registered: %s (version %s)", registered.name, registered.version
        )


# ---------------------------------------------------------------------------
# Null context helper
# ---------------------------------------------------------------------------
class _NullContext:
    """No-op context manager used when MLflow is unavailable."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sensor Anomaly AutoML Trainer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        choices=["sklearn", "azure"],
        default="sklearn",
        help="Training backend",
    )
    parser.add_argument(
        "--data",
        default="data/sample_sensor_data.csv",
        help="Path to sensor data CSV (sklearn mode)",
    )
    parser.add_argument(
        "--output-dir",
        default=MODEL_OUTPUT_DIR,
        help="Directory for saved model artefacts",
    )
    parser.add_argument(
        "--data-asset",
        default="sensor-data",
        help="AML data asset name (azure mode)",
    )
    parser.add_argument(
        "--max-trials",
        type=int,
        default=20,
        help="Max AutoML trials (azure mode)",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = _parse_args()

    if args.backend == "sklearn":
        trainer = SklearnAnomalyTrainer(output_dir=args.output_dir)
        model_path = trainer.run(args.data)
        logger.info("Training complete. Model saved to: %s", model_path)

    elif args.backend == "azure":
        trainer_azure = AzureAutoMLTrainer()
        job_name = trainer_azure.submit_job(
            data_asset_name=args.data_asset,
            max_trials=args.max_trials,
        )
        logger.info(
            "Azure AutoML job submitted: %s. "
            "Monitor at https://ml.azure.com",
            job_name,
        )


if __name__ == "__main__":
    main()
