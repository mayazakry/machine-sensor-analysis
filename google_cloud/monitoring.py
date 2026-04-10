"""
Cloud Monitoring – Custom Metrics & Alert Policies.

This module provides helpers for:
1. Writing custom time-series metrics (anomaly score, prediction latency,
   anomaly rate) to Cloud Monitoring.
2. Creating or updating Cloud Monitoring alert policies programmatically.

Usage:
    from monitoring import MonitoringClient, AlertPolicyManager

    mc = MonitoringClient(project_id="your-project-id")
    mc.write_anomaly_metric(sensor_id="machine-001", is_anomaly=True, score=-0.5)

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS – Service account key path
    PROJECT_ID                      – GCP project ID
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud import monitoring_v3
from google.protobuf import duration_pb2

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Metric type prefixes
# ---------------------------------------------------------------------------
METRIC_PREFIX = "custom.googleapis.com/sensor_anomaly"
METRIC_ANOMALY_SCORE = f"{METRIC_PREFIX}/score"
METRIC_IS_ANOMALY = f"{METRIC_PREFIX}/detected"
METRIC_PREDICTION_LATENCY = f"{METRIC_PREFIX}/prediction_latency_ms"
METRIC_ANOMALY_RATE = f"{METRIC_PREFIX}/rate"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class MetricPoint:
    """A single data point for a Cloud Monitoring time series."""

    metric_type: str
    value: float
    labels: dict[str, str]
    seconds: int | None = None
    nanos: int | None = None


# ---------------------------------------------------------------------------
# MonitoringClient
# ---------------------------------------------------------------------------


class MonitoringClient:
    """Thin wrapper around Cloud Monitoring that batches time-series writes.

    Args:
        project_id: GCP project ID.
    """

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.project_name = f"projects/{project_id}"
        self._client = monitoring_v3.MetricServiceClient()
        self._ensure_metric_descriptors()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_metric_descriptors(self) -> None:
        """Create custom metric descriptors if they do not already exist."""
        descriptors = [
            {
                "type": METRIC_ANOMALY_SCORE,
                "description": "Isolation Forest anomaly score for a sensor reading (higher = more anomalous).",
                "metric_kind": monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
                "value_type": monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
                "unit": "1",
                "labels": [
                    {"key": "sensor_id", "description": "Sensor / machine identifier"},
                    {"key": "region", "description": "GCP region"},
                ],
            },
            {
                "type": METRIC_IS_ANOMALY,
                "description": "Binary flag: 1 = anomaly detected, 0 = normal.",
                "metric_kind": monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
                "value_type": monitoring_v3.MetricDescriptor.ValueType.INT64,
                "unit": "1",
                "labels": [
                    {"key": "sensor_id", "description": "Sensor / machine identifier"},
                ],
            },
            {
                "type": METRIC_PREDICTION_LATENCY,
                "description": "End-to-end prediction latency in milliseconds.",
                "metric_kind": monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
                "value_type": monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
                "unit": "ms",
                "labels": [
                    {"key": "sensor_id", "description": "Sensor / machine identifier"},
                ],
            },
            {
                "type": METRIC_ANOMALY_RATE,
                "description": "Fraction of recent readings classified as anomalies.",
                "metric_kind": monitoring_v3.MetricDescriptor.MetricKind.GAUGE,
                "value_type": monitoring_v3.MetricDescriptor.ValueType.DOUBLE,
                "unit": "1",
                "labels": [
                    {"key": "sensor_id", "description": "Sensor / machine identifier"},
                ],
            },
        ]

        for desc_spec in descriptors:
            labels = [
                monitoring_v3.LabelDescriptor(**lbl)
                for lbl in desc_spec.pop("labels", [])
            ]
            descriptor = monitoring_v3.MetricDescriptor(labels=labels, **desc_spec)
            try:
                self._client.create_metric_descriptor(
                    name=self.project_name,
                    metric_descriptor=descriptor,
                )
                logger.info("Created metric descriptor: %s", descriptor.type)
            except AlreadyExists:
                logger.debug("Metric descriptor already exists: %s", descriptor.type)
            except GoogleAPICallError as exc:
                logger.warning("Could not create metric descriptor %s: %s", descriptor.type, exc)

    def _build_time_series(self, point: MetricPoint) -> monitoring_v3.TimeSeries:
        """Convert a :class:`MetricPoint` to a Cloud Monitoring TimeSeries proto."""
        now = point.seconds or int(time.time())
        nanos = point.nanos or int((time.time() - int(time.time())) * 1e9)

        series = monitoring_v3.TimeSeries()
        series.metric.type = point.metric_type
        for k, v in point.labels.items():
            series.metric.labels[k] = v

        series.resource.type = "global"
        series.resource.labels["project_id"] = self.project_id

        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": now, "nanos": nanos}}
        )

        if isinstance(point.value, int):
            data_point = monitoring_v3.Point(
                {"interval": interval, "value": {"int64_value": point.value}}
            )
        else:
            data_point = monitoring_v3.Point(
                {"interval": interval, "value": {"double_value": float(point.value)}}
            )

        series.points = [data_point]
        return series

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_points(self, points: list[MetricPoint]) -> None:
        """Write a batch of metric points to Cloud Monitoring.

        Args:
            points: List of :class:`MetricPoint` objects to write.
        """
        if not points:
            return

        time_series = [self._build_time_series(p) for p in points]
        try:
            self._client.create_time_series(
                name=self.project_name,
                time_series=time_series,
            )
            logger.debug("Wrote %d metric point(s).", len(points))
        except GoogleAPICallError as exc:
            logger.error("Failed to write %d metric point(s): %s", len(points), exc)

    def write_anomaly_metric(
        self,
        sensor_id: str,
        is_anomaly: bool,
        score: float | None = None,
        latency_ms: float | None = None,
        region: str = "global",
    ) -> None:
        """Write anomaly detection metrics for one sensor reading.

        Args:
            sensor_id: Sensor / machine identifier.
            is_anomaly: Whether the reading was classified as an anomaly.
            score: Raw Isolation Forest anomaly score (optional).
            latency_ms: Prediction end-to-end latency in ms (optional).
            region: GCP region label for the metric (optional).
        """
        points: list[MetricPoint] = []

        points.append(MetricPoint(
            metric_type=METRIC_IS_ANOMALY,
            value=1 if is_anomaly else 0,
            labels={"sensor_id": sensor_id},
        ))

        if score is not None:
            points.append(MetricPoint(
                metric_type=METRIC_ANOMALY_SCORE,
                value=float(score),
                labels={"sensor_id": sensor_id, "region": region},
            ))

        if latency_ms is not None:
            points.append(MetricPoint(
                metric_type=METRIC_PREDICTION_LATENCY,
                value=float(latency_ms),
                labels={"sensor_id": sensor_id},
            ))

        self.write_points(points)

    def write_anomaly_rate(self, sensor_id: str, rate: float) -> None:
        """Write the rolling anomaly rate (0–1) for *sensor_id*.

        Args:
            sensor_id: Sensor identifier.
            rate: Fraction of recent readings classified as anomalies.
        """
        self.write_points([
            MetricPoint(
                metric_type=METRIC_ANOMALY_RATE,
                value=rate,
                labels={"sensor_id": sensor_id},
            )
        ])


# ---------------------------------------------------------------------------
# AlertPolicyManager
# ---------------------------------------------------------------------------


class AlertPolicyManager:
    """Create and manage Cloud Monitoring alert policies.

    Args:
        project_id: GCP project ID.
    """

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.project_name = f"projects/{project_id}"
        self._client = monitoring_v3.AlertPolicyServiceClient()
        self._channel_client = monitoring_v3.NotificationChannelServiceClient()

    def create_email_notification_channel(self, email_address: str, display_name: str) -> str:
        """Create an email notification channel and return its resource name.

        Args:
            email_address: Destination email address for alert notifications.
            display_name: Human-readable name for the channel.

        Returns:
            Full resource name of the created channel.
        """
        channel = monitoring_v3.NotificationChannel(
            type_="email",
            display_name=display_name,
            labels={"email_address": email_address},
        )
        try:
            result = self._channel_client.create_notification_channel(
                name=self.project_name,
                notification_channel=channel,
            )
            logger.info("Created notification channel: %s", result.name)
            return result.name
        except GoogleAPICallError as exc:
            logger.error("Failed to create notification channel: %s", exc)
            raise

    def create_anomaly_rate_alert(
        self,
        display_name: str,
        threshold: float = 0.1,
        duration_seconds: int = 300,
        notification_channels: list[str] | None = None,
    ) -> monitoring_v3.AlertPolicy:
        """Create an alert policy that fires when the anomaly rate exceeds *threshold*.

        Args:
            display_name: Human-readable name for the alert policy.
            threshold: Anomaly rate fraction (0–1) above which the alert fires.
            duration_seconds: Consecutive seconds above threshold before firing.
            notification_channels: List of notification channel resource names.

        Returns:
            The created :class:`monitoring_v3.AlertPolicy`.
        """
        condition = monitoring_v3.AlertPolicy.Condition(
            display_name=f"Anomaly rate > {threshold * 100:.0f}%",
            condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                filter=(
                    f'metric.type="{METRIC_ANOMALY_RATE}" '
                    f'resource.type="global"'
                ),
                comparison=monitoring_v3.ComparisonType.COMPARISON_GT,
                threshold_value=threshold,
                duration=duration_pb2.Duration(seconds=duration_seconds),
                aggregations=[
                    monitoring_v3.Aggregation(
                        alignment_period=duration_pb2.Duration(seconds=60),
                        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                    )
                ],
            ),
        )

        policy = monitoring_v3.AlertPolicy(
            display_name=display_name,
            conditions=[condition],
            combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
            notification_channels=notification_channels or [],
            alert_strategy=monitoring_v3.AlertPolicy.AlertStrategy(
                auto_close=duration_pb2.Duration(seconds=1800),
            ),
        )

        try:
            result = self._client.create_alert_policy(
                name=self.project_name,
                alert_policy=policy,
            )
            logger.info("Alert policy created: %s", result.name)
            return result
        except AlreadyExists:
            logger.info("Alert policy '%s' already exists.", display_name)
            return policy
        except GoogleAPICallError as exc:
            logger.error("Failed to create alert policy: %s", exc)
            raise

    def create_high_anomaly_score_alert(
        self,
        display_name: str,
        threshold: float = 0.8,
        notification_channels: list[str] | None = None,
    ) -> monitoring_v3.AlertPolicy:
        """Create an alert that fires when the mean anomaly score exceeds *threshold*.

        Args:
            display_name: Policy display name.
            threshold: Mean anomaly score above which the alert fires.
            notification_channels: List of notification channel resource names.

        Returns:
            The created :class:`monitoring_v3.AlertPolicy`.
        """
        condition = monitoring_v3.AlertPolicy.Condition(
            display_name=f"Mean anomaly score > {threshold}",
            condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                filter=(
                    f'metric.type="{METRIC_ANOMALY_SCORE}" '
                    f'resource.type="global"'
                ),
                comparison=monitoring_v3.ComparisonType.COMPARISON_GT,
                threshold_value=threshold,
                duration=duration_pb2.Duration(seconds=60),
                aggregations=[
                    monitoring_v3.Aggregation(
                        alignment_period=duration_pb2.Duration(seconds=60),
                        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
                    )
                ],
            ),
        )

        policy = monitoring_v3.AlertPolicy(
            display_name=display_name,
            conditions=[condition],
            combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
            notification_channels=notification_channels or [],
        )

        try:
            result = self._client.create_alert_policy(
                name=self.project_name,
                alert_policy=policy,
            )
            logger.info("Alert policy created: %s", result.name)
            return result
        except AlreadyExists:
            logger.info("Alert policy '%s' already exists.", display_name)
            return policy
        except GoogleAPICallError as exc:
            logger.error("Failed to create alert policy: %s", exc)
            raise


# ---------------------------------------------------------------------------
# CLI / convenience runner
# ---------------------------------------------------------------------------


def setup_monitoring(project_id: str, alert_email: str | None = None) -> None:
    """Initialise monitoring metrics and alert policies for *project_id*.

    Args:
        project_id: GCP project ID.
        alert_email: If provided, create an email channel and wire it to
            the alert policies.
    """
    logger.info("Setting up Cloud Monitoring for project: %s", project_id)

    mc = MonitoringClient(project_id)
    apm = AlertPolicyManager(project_id)

    channels = []
    if alert_email:
        channel_name = apm.create_email_notification_channel(
            email_address=alert_email,
            display_name="Sensor Anomaly Alerts",
        )
        channels = [channel_name]

    apm.create_anomaly_rate_alert(
        display_name="Sensor Anomaly Rate Alert",
        threshold=0.1,
        duration_seconds=300,
        notification_channels=channels,
    )

    apm.create_high_anomaly_score_alert(
        display_name="High Anomaly Score Alert",
        threshold=0.8,
        notification_channels=channels,
    )

    logger.info("Cloud Monitoring setup complete.")


if __name__ == "__main__":
    import sys

    project = os.environ.get("PROJECT_ID", "your-project-id")
    email = sys.argv[1] if len(sys.argv) > 1 else None
    setup_monitoring(project, alert_email=email)
