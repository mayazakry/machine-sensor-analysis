"""
Cloud Pub/Sub Publisher for Machine Sensor Data.

This module simulates IoT sensor devices publishing readings to a
Google Cloud Pub/Sub topic for downstream processing by Dataflow.

Usage:
    python pubsub_publisher.py --project YOUR_PROJECT_ID --topic sensor-data

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON key file
    PROJECT_ID                     - Google Cloud project ID
    PUBSUB_TOPIC                   - Pub/Sub topic name (default: sensor-data)
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import pubsub_v1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULT_PROJECT_ID = os.environ.get("PROJECT_ID", "your-project-id")
DEFAULT_TOPIC = os.environ.get("PUBSUB_TOPIC", "sensor-data")
DEFAULT_SENSOR_IDS = [f"machine-{i:03d}" for i in range(1, 6)]
PUBLISH_INTERVAL_SECONDS = float(os.environ.get("PUBLISH_INTERVAL", "1.0"))


# ---------------------------------------------------------------------------
# Sensor simulation helpers
# ---------------------------------------------------------------------------

def generate_sensor_reading(sensor_id: str, inject_anomaly: bool = False) -> dict:
    """Generate a simulated sensor reading for a given machine.

    Args:
        sensor_id: Unique identifier for the sensor/machine.
        inject_anomaly: When ``True`` the reading will contain extreme values
            to simulate a fault condition.

    Returns:
        A dictionary with sensor fields ready to be serialised to JSON and
        published to Pub/Sub.
    """
    if inject_anomaly:
        # Simulate an extreme fault reading
        temperature = random.choice([random.uniform(80, 120), random.uniform(-20, 0)])
        vibration = random.uniform(5.0, 10.0)
        pressure = random.uniform(1200, 1500)
        value = random.choice([random.uniform(800, 1200), random.uniform(-600, -300)])
    else:
        # Normal operating range
        temperature = random.gauss(25.0, 1.5)
        vibration = random.gauss(0.12, 0.02)
        pressure = random.gauss(1013.25, 2.0)
        value = temperature  # convenience field used by downstream models

    return {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 4),
        "vibration": round(vibration, 4),
        "pressure": round(pressure, 4),
        "value": round(value, 4),
    }


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

class SensorPublisher:
    """Wraps ``pubsub_v1.PublisherClient`` with retry/backoff logic.

    Args:
        project_id: Google Cloud project ID.
        topic_name: Name of the Pub/Sub topic (not the full resource path).
    """

    def __init__(self, project_id: str, topic_name: str) -> None:
        self.project_id = project_id
        self.topic_name = topic_name
        self.client = pubsub_v1.PublisherClient()
        self.topic_path = self.client.topic_path(project_id, topic_name)
        self._ensure_topic()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_topic(self) -> None:
        """Create the Pub/Sub topic if it does not already exist."""
        try:
            self.client.get_topic(request={"topic": self.topic_path})
            logger.info("Topic already exists: %s", self.topic_path)
        except Exception:  # noqa: BLE001 – topic not found or permission error
            try:
                self.client.create_topic(request={"name": self.topic_path})
                logger.info("Created topic: %s", self.topic_path)
            except GoogleAPICallError as exc:
                logger.error("Failed to create topic %s: %s", self.topic_path, exc)
                raise

    def _on_publish_callback(self, future: pubsub_v1.futures.Future) -> None:
        """Callback invoked when a publish future resolves."""
        try:
            message_id = future.result(timeout=30)
            logger.debug("Published message id: %s", message_id)
        except (GoogleAPICallError, RetryError, TimeoutError) as exc:
            logger.error("Failed to publish message: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, reading: dict) -> None:
        """Serialise *reading* to JSON and publish it to the topic.

        The publish call is non-blocking; the result is handled via a
        callback so that high-throughput sensors are not bottlenecked.

        Args:
            reading: Sensor data dictionary as returned by
                :func:`generate_sensor_reading`.
        """
        payload = json.dumps(reading).encode("utf-8")

        # Attach ordering / filtering attributes for Dataflow
        attributes = {
            "sensor_id": reading["sensor_id"],
            "event_type": "sensor_reading",
        }

        future = self.client.publish(self.topic_path, payload, **attributes)
        future.add_done_callback(self._on_publish_callback)

    def publish_batch(self, sensor_ids: list, anomaly_rate: float = 0.05) -> None:
        """Publish one reading for each sensor in *sensor_ids*.

        Args:
            sensor_ids: List of sensor identifier strings.
            anomaly_rate: Fraction of messages that will contain injected
                anomalies (0–1).  Defaults to 5 %.
        """
        for sid in sensor_ids:
            inject = random.random() < anomaly_rate
            reading = generate_sensor_reading(sid, inject_anomaly=inject)
            self.publish(reading)
            if inject:
                logger.warning("Injected anomaly for sensor %s: %s", sid, reading)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish simulated machine sensor readings to Cloud Pub/Sub."
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT_ID, help="GCP project ID")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Pub/Sub topic name")
    parser.add_argument(
        "--interval",
        type=float,
        default=PUBLISH_INTERVAL_SECONDS,
        help="Seconds between publish rounds (default: 1.0)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Fraction of messages to inject as anomalies (default: 0.05)",
    )
    parser.add_argument(
        "--sensors",
        nargs="+",
        default=DEFAULT_SENSOR_IDS,
        help="List of sensor IDs to simulate",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Publish a single round and exit (useful for testing)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info(
        "Starting publisher | project=%s topic=%s interval=%.1fs sensors=%s",
        args.project,
        args.topic,
        args.interval,
        args.sensors,
    )

    publisher = SensorPublisher(project_id=args.project, topic_name=args.topic)

    try:
        if args.once:
            publisher.publish_batch(args.sensors, anomaly_rate=args.anomaly_rate)
            logger.info("Single publish round complete.")
        else:
            while True:
                publisher.publish_batch(args.sensors, anomaly_rate=args.anomaly_rate)
                logger.info(
                    "Published %d readings – sleeping %.1fs",
                    len(args.sensors),
                    args.interval,
                )
                time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Publisher stopped by user.")


if __name__ == "__main__":
    main()
