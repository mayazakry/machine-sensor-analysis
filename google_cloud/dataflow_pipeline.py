"""
Dataflow (Apache Beam) Pipeline – Pub/Sub → BigQuery.

This pipeline reads JSON sensor readings from a Cloud Pub/Sub subscription,
enriches each record, runs a lightweight threshold-based anomaly flag, and
writes the results to a BigQuery table.

Usage (local / Direct runner):
    python dataflow_pipeline.py \
        --project YOUR_PROJECT_ID \
        --subscription projects/YOUR_PROJECT_ID/subscriptions/sensor-data-sub \
        --bq_table YOUR_PROJECT_ID:sensor_dataset.raw_sensor_data \
        --runner DirectRunner

Usage (managed Dataflow):
    python dataflow_pipeline.py \
        --project YOUR_PROJECT_ID \
        --subscription projects/YOUR_PROJECT_ID/subscriptions/sensor-data-sub \
        --bq_table YOUR_PROJECT_ID:sensor_dataset.raw_sensor_data \
        --runner DataflowRunner \
        --region us-central1 \
        --temp_location gs://YOUR_BUCKET/temp \
        --staging_location gs://YOUR_BUCKET/staging \
        --job_name sensor-anomaly-pipeline

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON key file
    PROJECT_ID                     - Google Cloud project ID
"""

import json
import logging
import os
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
    WorkerOptions,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BigQuery table schema
# ---------------------------------------------------------------------------
SENSOR_TABLE_SCHEMA = {
    "fields": [
        {"name": "sensor_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "temperature", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "vibration", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "pressure", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "value", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "is_anomaly", "type": "BOOL", "mode": "NULLABLE"},
        {"name": "anomaly_reason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "pipeline_version", "type": "STRING", "mode": "NULLABLE"},
    ]
}

PIPELINE_VERSION = "1.0.0"

# Thresholds for the lightweight in-pipeline anomaly flag.
# These are intentionally conservative; the Vertex AI model provides the
# authoritative anomaly score.
TEMP_MIN, TEMP_MAX = -10.0, 80.0
VIBRATION_MAX = 3.0
PRESSURE_MIN, PRESSURE_MAX = 900.0, 1100.0
VALUE_MIN, VALUE_MAX = -500.0, 1000.0


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

class ParseJsonFn(beam.DoFn):
    """Deserialise a raw Pub/Sub message bytes payload into a Python dict.

    Messages that fail JSON parsing are emitted on a dedicated ``errors``
    side-output tag so they can be routed to a dead-letter sink without
    blocking the main pipeline.
    """

    ERRORS_TAG = "parse_errors"

    def process(self, element, timestamp=beam.DoFn.TimestampParam):  # noqa: ARG002
        try:
            record = json.loads(element.decode("utf-8"))
            yield record
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Failed to parse message: %s | error: %s", element, exc)
            yield beam.pvalue.TaggedOutput(
                self.ERRORS_TAG,
                {"raw": str(element), "error": str(exc)},
            )


class EnrichAndValidateFn(beam.DoFn):
    """Enrich sensor records and apply threshold-based anomaly flagging.

    Enrichment adds:
    - ``processed_at``: ISO-8601 UTC timestamp of when the record was processed.
    - ``pipeline_version``: Semver string for the current pipeline release.
    - ``is_anomaly``: Boolean flag set to ``True`` when any reading exceeds
      the configured safety thresholds.
    - ``anomaly_reason``: Human-readable description of which threshold(s)
      were breached, or ``None`` for normal records.
    """

    def process(self, element):
        record = dict(element)

        reasons = []
        temp = record.get("temperature")
        vib = record.get("vibration")
        pres = record.get("pressure")
        val = record.get("value")

        if temp is not None and not (TEMP_MIN <= temp <= TEMP_MAX):
            reasons.append(f"temperature={temp:.2f} outside [{TEMP_MIN},{TEMP_MAX}]")
        if vib is not None and vib > VIBRATION_MAX:
            reasons.append(f"vibration={vib:.4f} > {VIBRATION_MAX}")
        if pres is not None and not (PRESSURE_MIN <= pres <= PRESSURE_MAX):
            reasons.append(f"pressure={pres:.2f} outside [{PRESSURE_MIN},{PRESSURE_MAX}]")
        if val is not None and not (VALUE_MIN <= val <= VALUE_MAX):
            reasons.append(f"value={val:.2f} outside [{VALUE_MIN},{VALUE_MAX}]")

        record["is_anomaly"] = len(reasons) > 0
        record["anomaly_reason"] = "; ".join(reasons) if reasons else None
        record["processed_at"] = datetime.now(timezone.utc).isoformat()
        record["pipeline_version"] = PIPELINE_VERSION

        yield record


class FormatForBigQueryFn(beam.DoFn):
    """Cast/coerce record fields to types compatible with the BQ schema.

    Missing optional fields are filled with ``None`` so that BigQuery does
    not reject the row.
    """

    REQUIRED_FIELDS = {"sensor_id", "timestamp", "processed_at"}
    OPTIONAL_FIELDS = {
        "temperature", "vibration", "pressure", "value",
        "is_anomaly", "anomaly_reason", "pipeline_version",
    }

    def process(self, element):
        row = {}

        for field in self.REQUIRED_FIELDS:
            value = element.get(field)
            if value is None:
                logger.warning("Required field '%s' missing – dropping row: %s", field, element)
                return  # Drop row; do not yield
            row[field] = str(value)

        for field in self.OPTIONAL_FIELDS:
            row[field] = element.get(field)

        # Ensure numeric fields are float / bool
        for num_field in ("temperature", "vibration", "pressure", "value"):
            if row.get(num_field) is not None:
                try:
                    row[num_field] = float(row[num_field])
                except (TypeError, ValueError):
                    row[num_field] = None

        yield row


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(pipeline: beam.Pipeline, subscription: str, bq_table: str):
    """Attach all transforms to *pipeline* and return it.

    Args:
        pipeline: An initialised :class:`beam.Pipeline` instance.
        subscription: Full Pub/Sub subscription resource path,
            e.g. ``projects/my-project/subscriptions/sensor-data-sub``.
        bq_table: BigQuery table spec in ``project:dataset.table`` format.

    Returns:
        The modified pipeline (chained for convenience).
    """
    parsed, errors = (
        pipeline
        | "Read from Pub/Sub" >> ReadFromPubSub(subscription=subscription)
        | "Parse JSON" >> beam.ParDo(ParseJsonFn()).with_outputs(
            ParseJsonFn.ERRORS_TAG, main="valid"
        )
    )

    # ---------- Main path ------------------------------------------------
    (
        parsed
        | "Enrich & Validate" >> beam.ParDo(EnrichAndValidateFn())
        | "Format for BigQuery" >> beam.ParDo(FormatForBigQueryFn())
        | "Write to BigQuery" >> WriteToBigQuery(
            bq_table,
            schema=SENSOR_TABLE_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            # Retry on transient BQ errors
            insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
        )
    )

    # ---------- Dead-letter path -----------------------------------------
    (
        errors
        | "Log Parse Errors" >> beam.Map(
            lambda e: logger.error("Dead-letter record: %s", e)
        )
    )

    return pipeline


# ---------------------------------------------------------------------------
# Pipeline options builder
# ---------------------------------------------------------------------------

def build_options(args: list | None = None) -> PipelineOptions:
    """Build and return :class:`PipelineOptions` from CLI *args* (or ``sys.argv``)."""
    options = PipelineOptions(args)

    # Save main session so that globally-defined pickled lambdas work in
    # distributed workers.
    options.view_as(SetupOptions).save_main_session = True

    # Enable streaming mode (required for Pub/Sub sources).
    options.view_as(StandardOptions).streaming = True

    # Optimise worker configuration for streaming workloads.
    worker_opts = options.view_as(WorkerOptions)
    worker_opts.autoscaling_algorithm = "THROUGHPUT_BASED"
    worker_opts.max_num_workers = 10

    return options


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

def run(argv=None) -> None:
    """Build and run the Beam pipeline.

    Args:
        argv: Optional list of command-line argument strings.  If ``None``,
            ``sys.argv`` is used.
    """
    options = build_options(argv)
    gcp_opts = options.view_as(GoogleCloudOptions)

    # Pull convenience flags added by our own option class
    subscription = gcp_opts.subscription if hasattr(gcp_opts, "subscription") else None
    bq_table = gcp_opts.bq_table if hasattr(gcp_opts, "bq_table") else None

    # Fall back to environment variables if CLI flags were not provided
    if not subscription:
        project = os.environ.get("PROJECT_ID", "your-project-id")
        sub_name = os.environ.get("PUBSUB_SUBSCRIPTION", "sensor-data-sub")
        subscription = f"projects/{project}/subscriptions/{sub_name}"

    if not bq_table:
        project = os.environ.get("PROJECT_ID", "your-project-id")
        dataset = os.environ.get("BQ_DATASET", "sensor_dataset")
        bq_table = f"{project}:{dataset}.raw_sensor_data"

    logger.info("Starting Dataflow pipeline")
    logger.info("  Subscription : %s", subscription)
    logger.info("  BQ table     : %s", bq_table)
    logger.info("  Runner       : %s", options.view_as(StandardOptions).runner)

    with beam.Pipeline(options=options) as p:
        build_pipeline(p, subscription=subscription, bq_table=bq_table)

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    run()
