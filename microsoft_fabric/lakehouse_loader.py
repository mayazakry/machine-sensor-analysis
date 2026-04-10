"""
Lakehouse Loader - Load raw and processed sensor data to Microsoft Fabric Lakehouse.

Supports two loading strategies:
  1. Delta Lake via PySpark (for use inside Fabric Synapse notebooks / Spark pools).
  2. REST API via the OneLake / Lakehouse REST API (for use outside Fabric).

Usage (local / ACI):
    python lakehouse_loader.py --mode rest --source data/sample_sensor_data.csv

Usage (inside Synapse notebook):
    from lakehouse_loader import SparkLakehouseLoader
    loader = SparkLakehouseLoader(lakehouse_name="SensorLakehouse")
    loader.write_raw(df)
    loader.write_processed(processed_df)
"""

import os
import io
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
import numpy as np

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
# Configuration (loaded from env / config.yaml)
# ---------------------------------------------------------------------------
LAKEHOUSE_NAME = os.getenv("FABRIC_LAKEHOUSE_NAME", "SensorLakehouse")
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "")
LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID", "")
ONELAKE_ACCOUNT = os.getenv("ONELAKE_ACCOUNT_NAME", "onelake")
FABRIC_TOKEN = os.getenv("FABRIC_ACCESS_TOKEN", "")  # Azure AD token

RAW_TABLE = "raw_sensor_events"
PROCESSED_TABLE = "processed_sensor_events"
ANOMALY_TABLE = "anomaly_predictions"
PARTITION_COLUMN = "event_date"


# ---------------------------------------------------------------------------
# Spark-based Loader (Fabric Synapse / Spark pool)
# ---------------------------------------------------------------------------
class SparkLakehouseLoader:
    """Load DataFrames into Lakehouse Delta tables using PySpark.

    Designed to run inside a Microsoft Fabric Synapse Analytics notebook
    or a Fabric Spark pool where the ``spark`` session is already available.
    """

    def __init__(
        self,
        lakehouse_name: str = LAKEHOUSE_NAME,
        spark_session=None,
    ) -> None:
        """Initialize the loader.

        Args:
            lakehouse_name: Name of the Fabric Lakehouse.
            spark_session: Active SparkSession. If None, the global ``spark``
                variable (set by Fabric) is used.
        """
        self.lakehouse_name = lakehouse_name
        try:
            if spark_session is not None:
                self.spark = spark_session
            else:
                # Inside Fabric notebooks ``spark`` is a global variable
                import builtins  # noqa: PLC0415

                self.spark = getattr(builtins, "spark", None)
                if self.spark is None:
                    from pyspark.sql import SparkSession  # noqa: PLC0415

                    self.spark = SparkSession.builder.getOrCreate()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not initialize SparkSession: %s", exc)
            self.spark = None

        self.base_path = f"Tables/{lakehouse_name}"
        logger.info("SparkLakehouseLoader ready. Lakehouse=%s", lakehouse_name)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def _to_spark_df(self, df: "pd.DataFrame"):  # type: ignore[name-defined]
        """Convert pandas DataFrame to Spark DataFrame.

        Args:
            df: Input pandas DataFrame.

        Returns:
            PySpark DataFrame.
        """
        from pyspark.sql import functions as F  # noqa: PLC0415

        sdf = self.spark.createDataFrame(df)
        # Add partition column
        if "timestamp" in sdf.columns:
            sdf = sdf.withColumn(
                PARTITION_COLUMN, F.to_date(F.col("timestamp"))
            )
        else:
            sdf = sdf.withColumn(
                PARTITION_COLUMN, F.lit(datetime.now(timezone.utc).date().isoformat())
            )
        return sdf

    def _write_delta(
        self,
        df: "pd.DataFrame",
        table_name: str,
        mode: str = "append",
    ) -> None:
        """Write a DataFrame to a Lakehouse Delta table.

        Args:
            df: Data to write.
            table_name: Target Delta table name.
            mode: Write mode – ``append`` or ``overwrite``.
        """
        if self.spark is None:
            raise RuntimeError("SparkSession is not available.")
        sdf = self._to_spark_df(df)
        table_path = f"{self.base_path}/{table_name}"
        (
            sdf.write.format("delta")
            .mode(mode)
            .partitionBy(PARTITION_COLUMN)
            .save(table_path)
        )
        logger.info(
            "Written %d rows to Delta table '%s' (mode=%s)", len(df), table_name, mode
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def write_raw(self, df: "pd.DataFrame", mode: str = "append") -> None:
        """Persist raw sensor events to the Lakehouse.

        Args:
            df: Raw sensor DataFrame.
            mode: Spark write mode.
        """
        self._write_delta(df, RAW_TABLE, mode)

    def write_processed(self, df: "pd.DataFrame", mode: str = "append") -> None:
        """Persist processed / enriched sensor events.

        Args:
            df: Processed sensor DataFrame.
            mode: Spark write mode.
        """
        self._write_delta(df, PROCESSED_TABLE, mode)

    def write_predictions(self, df: "pd.DataFrame", mode: str = "append") -> None:
        """Persist model anomaly predictions.

        Args:
            df: Predictions DataFrame containing at least ``machine_id``,
                ``timestamp``, and ``is_anomaly`` columns.
            mode: Spark write mode.
        """
        self._write_delta(df, ANOMALY_TABLE, mode)

    def read_table(self, table_name: str) -> "pd.DataFrame":
        """Read a Lakehouse table into a pandas DataFrame.

        Args:
            table_name: Delta table name.

        Returns:
            pandas DataFrame.
        """
        if self.spark is None:
            raise RuntimeError("SparkSession is not available.")
        table_path = f"{self.base_path}/{table_name}"
        return self.spark.read.format("delta").load(table_path).toPandas()

    def optimize_table(self, table_name: str) -> None:
        """Run Delta OPTIMIZE + ZORDER on a table to improve query performance.

        Args:
            table_name: Target Delta table name.
        """
        if self.spark is None:
            raise RuntimeError("SparkSession is not available.")
        full_name = f"{self.lakehouse_name}.{table_name}"
        self.spark.sql(f"OPTIMIZE {full_name} ZORDER BY (machine_id, timestamp)")
        logger.info("Optimised table '%s'", full_name)

    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> None:
        """Remove old Delta files exceeding the retention window.

        Args:
            table_name: Target Delta table name.
            retention_hours: Files older than this are eligible for deletion.
        """
        if self.spark is None:
            raise RuntimeError("SparkSession is not available.")
        full_name = f"{self.lakehouse_name}.{table_name}"
        self.spark.sql(
            f"VACUUM {full_name} RETAIN {retention_hours} HOURS"
        )
        logger.info("Vacuumed table '%s' (retention=%dh)", full_name, retention_hours)


# ---------------------------------------------------------------------------
# REST / OneLake Loader (outside Fabric)
# ---------------------------------------------------------------------------
class RestLakehouseLoader:
    """Upload files/data to Fabric Lakehouse via the OneLake REST API.

    Suitable for use in ACI containers, local scripts, or Azure Functions
    that operate outside of a Spark environment.

    Authentication: Provide an Azure AD bearer token via the
    FABRIC_ACCESS_TOKEN environment variable or the ``token`` constructor
    argument.
    """

    def __init__(
        self,
        workspace_id: str = WORKSPACE_ID,
        lakehouse_id: str = LAKEHOUSE_ID,
        token: Optional[str] = None,
    ) -> None:
        """Initialize the REST loader.

        Args:
            workspace_id: Fabric Workspace GUID.
            lakehouse_id: Fabric Lakehouse GUID.
            token: Azure AD bearer token. Falls back to FABRIC_ACCESS_TOKEN env var.
        """
        try:
            import requests  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "requests library required. Run: pip install requests"
            )

        if not workspace_id or not lakehouse_id:
            raise ValueError(
                "FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_ID must be set."
            )

        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self._token = token or FABRIC_TOKEN
        self._base_url = (
            f"https://api.fabric.microsoft.com/v1"
            f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        )
        import requests  # noqa: PLC0415

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/octet-stream",
            }
        )
        logger.info(
            "RestLakehouseLoader ready. Workspace=%s, Lakehouse=%s",
            workspace_id,
            lakehouse_id,
        )

    def _upload_parquet(self, df: pd.DataFrame, path: str) -> None:
        """Serialise a DataFrame to Parquet and upload to OneLake.

        Args:
            df: Data to upload.
            path: Destination path within the Lakehouse Files section.
        """
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        url = f"{self._base_url}/files/{path}"
        response = self._session.put(url, data=buf.read())
        response.raise_for_status()
        logger.info("Uploaded %d rows to OneLake path '%s'", len(df), path)

    def upload_raw(self, df: pd.DataFrame) -> None:
        """Upload raw sensor data as Parquet to the Lakehouse Files area.

        Args:
            df: Raw sensor DataFrame.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        ts = int(datetime.now(timezone.utc).timestamp())
        path = f"raw_sensor_events/{date_str}/{ts}.parquet"
        self._upload_parquet(df, path)

    def upload_processed(self, df: pd.DataFrame) -> None:
        """Upload processed sensor data to the Lakehouse Files area.

        Args:
            df: Processed sensor DataFrame.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        ts = int(datetime.now(timezone.utc).timestamp())
        path = f"processed_sensor_events/{date_str}/{ts}.parquet"
        self._upload_parquet(df, path)

    def upload_predictions(self, df: pd.DataFrame) -> None:
        """Upload anomaly predictions to the Lakehouse Files area.

        Args:
            df: Predictions DataFrame.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        ts = int(datetime.now(timezone.utc).timestamp())
        path = f"anomaly_predictions/{date_str}/{ts}.parquet"
        self._upload_parquet(df, path)


# ---------------------------------------------------------------------------
# Data Enrichment Utilities
# ---------------------------------------------------------------------------
def enrich_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich raw sensor data with computed features.

    Adds rolling statistics, z-score anomaly flags, and derived columns
    commonly required by downstream ML models.

    Args:
        df: Raw sensor DataFrame. Must contain ``timestamp`` and sensor
            value columns.

    Returns:
        Enriched DataFrame with additional feature columns.
    """
    enriched = df.copy()

    # Ensure timestamp is parsed
    if "timestamp" in enriched.columns:
        enriched["timestamp"] = pd.to_datetime(enriched["timestamp"], utc=True)
        enriched = enriched.sort_values("timestamp").reset_index(drop=True)
        enriched["hour_of_day"] = enriched["timestamp"].dt.hour
        enriched["day_of_week"] = enriched["timestamp"].dt.dayofweek

    numeric_cols = enriched.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [
        c
        for c in numeric_cols
        if c
        not in (
            "hour_of_day",
            "day_of_week",
            "is_simulated_anomaly",
        )
    ]

    for col in numeric_cols:
        enriched[f"{col}_rolling_mean"] = (
            enriched[col].rolling(window=10, min_periods=1).mean()
        )
        enriched[f"{col}_rolling_std"] = (
            enriched[col].rolling(window=10, min_periods=1).std().fillna(0)
        )
        col_mean = enriched[col].mean()
        col_std = enriched[col].std() or 1e-9
        enriched[f"{col}_zscore"] = (enriched[col] - col_mean) / col_std

    enriched["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    return enriched


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fabric Lakehouse Loader",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["rest", "spark"],
        default="rest",
        help="Loading mode",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to source CSV file",
    )
    parser.add_argument(
        "--table",
        choices=["raw", "processed", "predictions"],
        default="raw",
        help="Target Lakehouse table",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = _parse_args()
    df = pd.read_csv(args.source)
    logger.info("Loaded %d rows from '%s'", len(df), args.source)

    if args.table == "processed":
        df = enrich_sensor_data(df)

    if args.mode == "rest":
        loader: RestLakehouseLoader = RestLakehouseLoader()
        upload_fn = {
            "raw": loader.upload_raw,
            "processed": loader.upload_processed,
            "predictions": loader.upload_predictions,
        }[args.table]
        upload_fn(df)
    else:
        loader_spark: SparkLakehouseLoader = SparkLakehouseLoader()
        write_fn = {
            "raw": loader_spark.write_raw,
            "processed": loader_spark.write_processed,
            "predictions": loader_spark.write_predictions,
        }[args.table]
        write_fn(df)

    logger.info("Load complete.")


if __name__ == "__main__":
    main()
