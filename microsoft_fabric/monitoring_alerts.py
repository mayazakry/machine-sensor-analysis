"""
Monitoring & Alerts - Azure Monitor and Microsoft Teams integration.

This module provides:
  1. Custom metric publishing to Azure Monitor.
  2. Alert rule management via Azure Monitor REST API.
  3. Proactive anomaly notifications to Microsoft Teams via Incoming Webhooks.
  4. Email alerts via Azure Communication Services.
  5. Scheduled health check loop.

Environment Variables:
    AZURE_SUBSCRIPTION_ID       : Azure subscription ID
    AZURE_RESOURCE_GROUP        : Resource group name
    AZURE_MONITOR_WORKSPACE_ID  : Log Analytics workspace ID
    TEAMS_WEBHOOK_URL           : Teams Incoming Webhook URL
    ACS_CONNECTION_STRING       : Azure Communication Services connection string
    ALERT_EMAIL_FROM            : Sender email address
    ALERT_EMAIL_TO              : Comma-separated recipient addresses
    ANOMALY_ALERT_THRESHOLD     : Fraction of anomalies that triggers an alert (default: 0.10)

Usage:
    python monitoring_alerts.py --mode monitor
    python monitoring_alerts.py --mode test-teams
    python monitoring_alerts.py --mode test-email
"""

import os
import json
import time
import logging
import argparse
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
import requests

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
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "")
AZURE_MONITOR_WORKSPACE_ID = os.getenv("AZURE_MONITOR_WORKSPACE_ID", "")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL", "")
ACS_CONNECTION_STRING = os.getenv("ACS_CONNECTION_STRING", "")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "alerts@company.com")
ALERT_EMAIL_TO = [
    e.strip() for e in os.getenv("ALERT_EMAIL_TO", "").split(",") if e.strip()
]
ANOMALY_ALERT_THRESHOLD = float(os.getenv("ANOMALY_ALERT_THRESHOLD", "0.10"))
MONITOR_INTERVAL_SECONDS = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))
REST_API_URL = os.getenv("REST_API_URL", "http://localhost:8000")


# ---------------------------------------------------------------------------
# Azure Monitor Custom Metrics Publisher
# ---------------------------------------------------------------------------
class AzureMonitorPublisher:
    """Publish custom metrics to Azure Monitor using the Data Ingestion API.

    Requires an Azure AD token with the ``Monitoring Metrics Publisher`` role
    on the target resource.
    """

    def __init__(
        self,
        resource_id: Optional[str] = None,
        token_provider=None,
    ) -> None:
        """Initialize the publisher.

        Args:
            resource_id: Full ARM resource ID of the monitored resource.
            token_provider: Callable returning a fresh Bearer token string.
                            Defaults to DefaultAzureCredential.
        """
        self.resource_id = resource_id or (
            f"/subscriptions/{AZURE_SUBSCRIPTION_ID}"
            f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
            f"/providers/Microsoft.Fabric/workspaces/sensor-workspace"
        )
        self._token_provider = token_provider
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _get_token(self) -> str:
        """Retrieve an Azure AD access token.

        Returns:
            Bearer token string.
        """
        if self._token_provider:
            return self._token_provider()
        try:
            from azure.identity import DefaultAzureCredential  # noqa: PLC0415

            cred = DefaultAzureCredential()
            token = cred.get_token("https://monitor.azure.com/.default")
            return token.token
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to obtain Azure AD token: %s", exc)
            raise

    def publish_metrics(
        self,
        metrics: Dict[str, float],
        namespace: str = "SensorAnomalyDetection",
        dimension_name: str = "Environment",
        dimension_value: str = "production",
    ) -> None:
        """Publish a dictionary of metric name → value pairs to Azure Monitor.

        Args:
            metrics: Mapping of metric names to numeric values.
            namespace: Custom metric namespace.
            dimension_name: Metric dimension name.
            dimension_value: Metric dimension value.
        """
        token = self._get_token()
        self._session.headers.update({"Authorization": f"Bearer {token}"})

        region = os.getenv("AZURE_REGION", "eastus")
        url = (
            f"https://{region}.monitoring.azure.com"
            f"{self.resource_id}/metrics"
        )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        series = []
        for name, value in metrics.items():
            series.append(
                {
                    "name": name,
                    "namespace": namespace,
                    "dims": {dimension_name: dimension_value},
                    "min": value,
                    "max": value,
                    "sum": value,
                    "count": 1,
                }
            )

        payload = {"time": now, "data": {"baseData": {"series": series}}}
        resp = self._session.post(url, json=payload, timeout=10)
        if resp.status_code not in (200, 202):
            logger.warning(
                "Azure Monitor publish failed: %d – %s", resp.status_code, resp.text
            )
        else:
            logger.info("Published %d metrics to Azure Monitor", len(metrics))


# ---------------------------------------------------------------------------
# Teams Notification
# ---------------------------------------------------------------------------
class TeamsNotifier:
    """Send rich adaptive card notifications to Microsoft Teams.

    Uses the Teams Incoming Webhook connector.  Create a webhook in Teams:
    Channel → Connectors → Incoming Webhook → configure → copy URL.
    """

    def __init__(self, webhook_url: str = TEAMS_WEBHOOK_URL) -> None:
        """Initialize the notifier.

        Args:
            webhook_url: Teams Incoming Webhook URL.
        """
        if not webhook_url:
            raise ValueError(
                "TEAMS_WEBHOOK_URL environment variable is required."
            )
        self.webhook_url = webhook_url
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _build_card(
        self,
        title: str,
        summary: str,
        details: Dict[str, Any],
        severity: str = "warning",
    ) -> Dict[str, Any]:
        """Build a Teams adaptive card payload.

        Args:
            title: Card title.
            summary: Short summary text.
            details: Key-value details to display in the card body.
            severity: One of ``info``, ``warning``, or ``critical``.

        Returns:
            Teams message card JSON payload.
        """
        colour_map = {
            "info": "0078D7",
            "warning": "FFA500",
            "critical": "D40000",
        }
        colour = colour_map.get(severity, "0078D7")
        facts = [{"name": k, "value": str(v)} for k, v in details.items()]
        return {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": colour,
            "summary": summary,
            "sections": [
                {
                    "activityTitle": f"🚨 {title}",
                    "activitySubtitle": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    ),
                    "facts": facts,
                    "markdown": True,
                }
            ],
        }

    def send_anomaly_alert(
        self,
        machine_id: str,
        anomaly_score: float,
        sensor_values: Dict[str, float],
        severity: str = "warning",
    ) -> bool:
        """Send an anomaly detection alert to Teams.

        Args:
            machine_id: Affected machine identifier.
            anomaly_score: Model anomaly score (higher = more anomalous).
            sensor_values: Current sensor readings.
            severity: Alert severity level.

        Returns:
            True if the notification was sent successfully.
        """
        details = {
            "Machine ID": machine_id,
            "Anomaly Score": f"{anomaly_score:.4f}",
            "Time": datetime.now(timezone.utc).isoformat(),
            **{k.capitalize(): f"{v:.2f}" for k, v in sensor_values.items()},
        }
        card = self._build_card(
            title="Machine Sensor Anomaly Detected",
            summary=f"Anomaly detected on {machine_id}",
            details=details,
            severity=severity,
        )
        resp = self._session.post(self.webhook_url, json=card, timeout=10)
        success = resp.status_code == 200
        if success:
            logger.info("Teams alert sent for machine '%s'", machine_id)
        else:
            logger.error(
                "Teams alert failed: %d – %s", resp.status_code, resp.text
            )
        return success

    def send_summary_report(
        self,
        period_minutes: int,
        total_events: int,
        anomaly_count: int,
        anomaly_rate: float,
    ) -> bool:
        """Send a periodic summary report to Teams.

        Args:
            period_minutes: Duration of the reporting window.
            total_events: Total events processed.
            anomaly_count: Number of anomalies detected.
            anomaly_rate: Fraction of anomalous events.

        Returns:
            True if the notification was sent successfully.
        """
        severity = "critical" if anomaly_rate >= ANOMALY_ALERT_THRESHOLD * 2 else (
            "warning" if anomaly_rate >= ANOMALY_ALERT_THRESHOLD else "info"
        )
        details = {
            "Period": f"Last {period_minutes} minutes",
            "Total Events": total_events,
            "Anomalies": anomaly_count,
            "Anomaly Rate": f"{anomaly_rate:.1%}",
            "Status": "⚠️ Elevated" if anomaly_rate >= ANOMALY_ALERT_THRESHOLD else "✅ Normal",
        }
        card = self._build_card(
            title="Sensor Monitoring Summary",
            summary=f"Anomaly rate: {anomaly_rate:.1%}",
            details=details,
            severity=severity,
        )
        resp = self._session.post(self.webhook_url, json=card, timeout=10)
        success = resp.status_code == 200
        if success:
            logger.info(
                "Teams summary sent (anomaly_rate=%.2f%%)", anomaly_rate * 100
            )
        return success

    def test_connection(self) -> bool:
        """Send a test notification to verify the webhook is working.

        Returns:
            True if the test was successful.
        """
        return self.send_summary_report(
            period_minutes=0,
            total_events=0,
            anomaly_count=0,
            anomaly_rate=0.0,
        )


# ---------------------------------------------------------------------------
# Email Alerts via Azure Communication Services
# ---------------------------------------------------------------------------
class EmailAlerter:
    """Send email alerts via Azure Communication Services.

    Requires the ``azure-communication-email`` SDK and a valid ACS resource.
    """

    def __init__(
        self,
        connection_string: str = ACS_CONNECTION_STRING,
        sender: str = ALERT_EMAIL_FROM,
        recipients: Optional[List[str]] = None,
    ) -> None:
        """Initialize the email alerter.

        Args:
            connection_string: ACS resource connection string.
            sender: From email address (must be verified in ACS).
            recipients: List of recipient email addresses.
        """
        try:
            from azure.communication.email import EmailClient  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "azure-communication-email required. "
                "Run: pip install azure-communication-email"
            )

        if not connection_string:
            raise ValueError("ACS_CONNECTION_STRING environment variable is required")

        from azure.communication.email import EmailClient  # noqa: PLC0415

        self.client = EmailClient.from_connection_string(connection_string)
        self.sender = sender
        self.recipients = recipients or ALERT_EMAIL_TO
        if not self.recipients:
            raise ValueError("ALERT_EMAIL_TO environment variable must be set")

    def send_anomaly_alert(
        self,
        machine_id: str,
        anomaly_score: float,
        sensor_values: Dict[str, float],
    ) -> None:
        """Send an anomaly alert email.

        Args:
            machine_id: Affected machine identifier.
            anomaly_score: Model anomaly score.
            sensor_values: Current sensor readings.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        sensor_rows = "".join(
            f"<tr><td>{k.capitalize()}</td><td>{v:.2f}</td></tr>"
            for k, v in sensor_values.items()
        )
        html_body = f"""
        <html><body>
        <h2 style="color:#D40000">⚠️ Machine Sensor Anomaly Detected</h2>
        <p><strong>Machine ID:</strong> {machine_id}</p>
        <p><strong>Time:</strong> {timestamp}</p>
        <p><strong>Anomaly Score:</strong> {anomaly_score:.4f}</p>
        <h3>Sensor Readings:</h3>
        <table border="1" cellpadding="5">
            <tr><th>Sensor</th><th>Value</th></tr>
            {sensor_rows}
        </table>
        <p>Please investigate the affected machine immediately.</p>
        </body></html>
        """
        message = {
            "senderAddress": self.sender,
            "recipients": {
                "to": [{"address": addr} for addr in self.recipients]
            },
            "content": {
                "subject": f"[ALERT] Anomaly detected on {machine_id}",
                "html": html_body,
            },
        }
        poller = self.client.begin_send(message)
        result = poller.result()
        logger.info("Email sent. Message ID: %s", result.get("id"))


# ---------------------------------------------------------------------------
# Alert Rule Manager
# ---------------------------------------------------------------------------
class AlertRuleManager:
    """Create and manage Azure Monitor alert rules programmatically."""

    def __init__(
        self,
        subscription_id: str = AZURE_SUBSCRIPTION_ID,
        resource_group: str = AZURE_RESOURCE_GROUP,
    ) -> None:
        try:
            from azure.identity import DefaultAzureCredential  # noqa: PLC0415
            from azure.mgmt.monitor import MonitorManagementClient  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "azure-mgmt-monitor required. Run: pip install azure-mgmt-monitor"
            )

        from azure.identity import DefaultAzureCredential  # noqa: PLC0415
        from azure.mgmt.monitor import MonitorManagementClient  # noqa: PLC0415

        credential = DefaultAzureCredential()
        self.client = MonitorManagementClient(credential, subscription_id)
        self.resource_group = resource_group
        self.subscription_id = subscription_id

    def create_metric_alert(
        self,
        alert_name: str,
        resource_id: str,
        metric_name: str,
        threshold: float,
        action_group_id: str,
        operator: str = "GreaterThan",
        aggregation: str = "Average",
        window_minutes: int = 5,
        severity: int = 2,
    ) -> None:
        """Create or update a metric alert rule.

        Args:
            alert_name: Alert rule name.
            resource_id: ARM resource ID to monitor.
            metric_name: Azure Monitor metric name.
            threshold: Alert threshold value.
            action_group_id: ARM ID of the action group to notify.
            operator: Comparison operator (GreaterThan / LessThan / etc.).
            aggregation: Metric aggregation type.
            window_minutes: Evaluation window in minutes.
            severity: Alert severity (0=critical, 4=verbose).
        """
        from azure.mgmt.monitor.models import (  # noqa: PLC0415
            MetricAlertResource,
            MetricAlertSingleResourceMultipleMetricCriteria,
            MetricCriteria,
            MetricAlertAction,
        )

        criteria = MetricAlertSingleResourceMultipleMetricCriteria(
            all_of=[
                MetricCriteria(
                    name="AnomalyRateCriteria",
                    metric_name=metric_name,
                    operator=operator,
                    threshold=threshold,
                    time_aggregation=aggregation,
                )
            ]
        )
        alert = MetricAlertResource(
            location="global",
            description=f"Alert: {metric_name} {operator} {threshold}",
            severity=severity,
            enabled=True,
            scopes=[resource_id],
            evaluation_frequency="PT1M",
            window_size=f"PT{window_minutes}M",
            criteria=criteria,
            actions=[MetricAlertAction(action_group_id=action_group_id)],
        )
        self.client.metric_alerts.create_or_update(
            self.resource_group, alert_name, alert
        )
        logger.info("Alert rule '%s' created/updated", alert_name)


# ---------------------------------------------------------------------------
# Health Monitor Loop
# ---------------------------------------------------------------------------
class HealthMonitor:
    """Continuously poll the REST API and publish metrics to Azure Monitor / Teams."""

    def __init__(
        self,
        api_url: str = REST_API_URL,
        teams_notifier: Optional[TeamsNotifier] = None,
        azure_publisher: Optional[AzureMonitorPublisher] = None,
        interval: int = MONITOR_INTERVAL_SECONDS,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.teams = teams_notifier
        self.azure = azure_publisher
        self.interval = interval
        self._window: List[Dict[str, Any]] = []  # sliding window of events
        self._window_start = datetime.now(timezone.utc)

    def _collect_metrics(self) -> Dict[str, float]:
        """Query the /metrics endpoint of the REST API.

        Returns:
            Dictionary of metric name → value.
        """
        try:
            resp = requests.get(f"{self.api_url}/metrics", timeout=5)
            resp.raise_for_status()
            data = resp.json()
            return {
                "requests_total": float(data.get("requests_total", 0)),
                "anomalies_detected": float(data.get("anomalies_detected", 0)),
                "errors_total": float(data.get("errors_total", 0)),
                "uptime_seconds": float(data.get("uptime_seconds", 0)),
            }
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to collect metrics: %s", exc)
            return {}

    def _check_anomaly_rate(self, metrics: Dict[str, float]) -> None:
        """Trigger alerts if anomaly rate exceeds the threshold.

        Args:
            metrics: Latest metrics from the REST API.
        """
        total = metrics.get("requests_total", 0)
        anomalies = metrics.get("anomalies_detected", 0)
        if total == 0:
            return
        rate = anomalies / total
        if rate >= ANOMALY_ALERT_THRESHOLD and self.teams:
            elapsed = (datetime.now(timezone.utc) - self._window_start).seconds // 60
            try:
                self.teams.send_summary_report(
                    period_minutes=elapsed,
                    total_events=int(total),
                    anomaly_count=int(anomalies),
                    anomaly_rate=rate,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to send Teams alert: %s", exc)

    def run(self) -> None:
        """Start the monitoring loop. Blocks indefinitely."""
        logger.info(
            "Starting health monitor (interval=%ds, threshold=%.0f%%)",
            self.interval,
            ANOMALY_ALERT_THRESHOLD * 100,
        )
        while True:
            metrics = self._collect_metrics()
            if metrics:
                if self.azure:
                    try:
                        self.azure.publish_metrics(metrics)
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Azure Monitor publish error: %s", exc)
                self._check_anomaly_rate(metrics)
                logger.info("Metrics: %s", metrics)
            time.sleep(self.interval)


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sensor Monitoring & Alerts",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["monitor", "test-teams", "test-email", "create-alerts"],
        default="monitor",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = _parse_args()

    if args.mode == "test-teams":
        notifier = TeamsNotifier()
        ok = notifier.test_connection()
        print("Teams test:", "✅ Success" if ok else "❌ Failed")

    elif args.mode == "test-email":
        alerter = EmailAlerter()
        alerter.send_anomaly_alert(
            machine_id="TEST_MACHINE",
            anomaly_score=0.9876,
            sensor_values={"temperature": 120.0, "vibration": 4.5},
        )
        print("Email test sent")

    elif args.mode == "create-alerts":
        manager = AlertRuleManager()
        manager.create_metric_alert(
            alert_name="high-anomaly-rate-alert",
            resource_id=(
                f"/subscriptions/{AZURE_SUBSCRIPTION_ID}"
                f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
                f"/providers/Microsoft.ContainerInstance/containerGroups/sensor-api"
            ),
            metric_name="anomalies_detected",
            threshold=50.0,
            action_group_id=(
                f"/subscriptions/{AZURE_SUBSCRIPTION_ID}"
                f"/resourceGroups/{AZURE_RESOURCE_GROUP}"
                f"/providers/Microsoft.Insights/actionGroups/sensor-alerts"
            ),
        )

    else:  # monitor
        teams: Optional[TeamsNotifier] = None
        if TEAMS_WEBHOOK_URL:
            teams = TeamsNotifier()
        azure: Optional[AzureMonitorPublisher] = None
        if AZURE_SUBSCRIPTION_ID:
            azure = AzureMonitorPublisher()
        monitor = HealthMonitor(teams_notifier=teams, azure_publisher=azure)
        monitor.run()


if __name__ == "__main__":
    main()
