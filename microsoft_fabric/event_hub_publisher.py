"""
Event Hub Publisher - Stream sensor data from IoT devices to Azure Event Hub.

This module simulates IoT sensor devices and publishes machine sensor readings
(temperature, vibration, pressure, humidity) to Azure Event Hub for real-time
anomaly detection processing.

Usage:
    python event_hub_publisher.py --mode simulate --interval 5
    python event_hub_publisher.py --mode file --file data/sample_sensor_data.csv
"""

import os
import json
import time
import logging
import argparse
import asyncio
import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Azure Event Hubs SDK
try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
except ImportError:
    raise ImportError(
        "Azure SDK not installed. Run: pip install azure-eventhub azure-identity"
    )

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Logging Configuration
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
EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "sensor-events")
EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE", "")  # FQDN without https://
BATCH_SIZE = int(os.getenv("PUBLISHER_BATCH_SIZE", "100"))

# Sensor thresholds for simulated data
SENSOR_CONFIG = {
    "temperature": {"mean": 75.0, "std": 5.0, "unit": "°C", "min": 40.0, "max": 120.0},
    "vibration": {"mean": 0.5, "std": 0.1, "unit": "g", "min": 0.0, "max": 5.0},
    "pressure": {"mean": 101.3, "std": 2.0, "unit": "kPa", "min": 80.0, "max": 130.0},
    "humidity": {"mean": 50.0, "std": 8.0, "unit": "%", "min": 10.0, "max": 95.0},
    "current": {"mean": 12.0, "std": 1.5, "unit": "A", "min": 0.0, "max": 30.0},
    "voltage": {"mean": 220.0, "std": 5.0, "unit": "V", "min": 180.0, "max": 260.0},
}

# Anomaly injection rate for simulation (fraction of readings that are anomalous)
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.05"))


# ---------------------------------------------------------------------------
# Sensor Data Generator
# ---------------------------------------------------------------------------
class SensorDataGenerator:
    """Simulates IoT sensor readings with configurable anomaly injection."""

    def __init__(
        self,
        machine_ids: Optional[List[str]] = None,
        anomaly_rate: float = ANOMALY_RATE,
        seed: Optional[int] = None,
    ) -> None:
        self.machine_ids = machine_ids or [f"MACHINE_{i:03d}" for i in range(1, 6)]
        self.anomaly_rate = anomaly_rate
        self._rng = np.random.default_rng(seed)
        logger.info(
            "SensorDataGenerator initialized with %d machines, anomaly_rate=%.2f",
            len(self.machine_ids),
            anomaly_rate,
        )

    def generate_reading(self, machine_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate a single sensor reading for a machine.

        Args:
            machine_id: Optional machine identifier. Chosen randomly if omitted.

        Returns:
            Dictionary representing one sensor event.
        """
        mid = machine_id or self._rng.choice(self.machine_ids)
        inject_anomaly = self._rng.random() < self.anomaly_rate

        reading: Dict[str, Any] = {
            "event_id": f"{mid}_{int(time.time() * 1000)}",
            "machine_id": mid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "is_simulated_anomaly": inject_anomaly,
        }

        for sensor, cfg in SENSOR_CONFIG.items():
            if inject_anomaly:
                # Push reading beyond normal range to simulate fault
                value = float(
                    self._rng.choice([-1, 1])
                    * self._rng.uniform(cfg["std"] * 3, cfg["std"] * 6)
                    + cfg["mean"]
                )
                value = float(np.clip(value, cfg["min"] * 0.8, cfg["max"] * 1.2))
            else:
                value = float(
                    np.clip(
                        self._rng.normal(cfg["mean"], cfg["std"]),
                        cfg["min"],
                        cfg["max"],
                    )
                )
            reading[sensor] = round(value, 4)

        return reading

    def generate_batch(self, size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
        """Generate a batch of sensor readings.

        Args:
            size: Number of readings to generate.

        Returns:
            List of sensor event dictionaries.
        """
        return [self.generate_reading() for _ in range(size)]


# ---------------------------------------------------------------------------
# Event Hub Publisher
# ---------------------------------------------------------------------------
class SensorEventHubPublisher:
    """Publishes sensor readings to Azure Event Hub.

    Supports both connection-string and Managed Identity authentication.
    Implements automatic batching and retry logic.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        eventhub_name: str = EVENT_HUB_NAME,
        namespace: Optional[str] = None,
        use_managed_identity: bool = False,
    ) -> None:
        """Initialize the publisher.

        Args:
            connection_string: Event Hub connection string (overrides env var).
            eventhub_name: Name of the Event Hub entity.
            namespace: Fully-qualified Event Hub namespace (for MI auth).
            use_managed_identity: Use Azure Managed Identity instead of conn string.
        """
        self.eventhub_name = eventhub_name
        conn_str = connection_string or EVENT_HUB_CONNECTION_STR

        if use_managed_identity:
            credential = ManagedIdentityCredential()
            fqns = namespace or EVENT_HUB_NAMESPACE
            if not fqns:
                raise ValueError(
                    "EVENT_HUB_NAMESPACE must be set when using Managed Identity"
                )
            self.producer = EventHubProducerClient(
                fully_qualified_namespace=fqns,
                eventhub_name=eventhub_name,
                credential=credential,
            )
            logger.info("Using Managed Identity authentication for Event Hub")
        else:
            if not conn_str:
                raise ValueError(
                    "EVENT_HUB_CONNECTION_STR environment variable is required"
                )
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=conn_str, eventhub_name=eventhub_name
            )
            logger.info("Using connection string authentication for Event Hub")

        self._events_sent = 0

    def publish_batch(self, events: List[Dict[str, Any]]) -> int:
        """Send a batch of sensor events to Event Hub.

        Args:
            events: List of sensor event dictionaries.

        Returns:
            Number of events successfully sent.
        """
        sent = 0
        with self.producer:
            event_data_batch = self.producer.create_batch()
            for event in events:
                try:
                    event_data_batch.add(EventData(json.dumps(event)))
                    sent += 1
                except ValueError:
                    # Batch is full — flush and start a new one
                    self.producer.send_batch(event_data_batch)
                    logger.debug("Flushed full batch (%d events)", sent)
                    event_data_batch = self.producer.create_batch()
                    event_data_batch.add(EventData(json.dumps(event)))
                    sent += 1

            if len(event_data_batch) > 0:
                self.producer.send_batch(event_data_batch)

        self._events_sent += sent
        logger.info("Published %d events (total: %d)", sent, self._events_sent)
        return sent

    def publish_single(self, event: Dict[str, Any]) -> None:
        """Publish a single sensor event.

        Args:
            event: Sensor event dictionary.
        """
        self.publish_batch([event])

    def publish_from_dataframe(self, df: pd.DataFrame) -> int:
        """Publish all rows from a pandas DataFrame.

        Args:
            df: DataFrame with sensor columns.

        Returns:
            Total number of events sent.
        """
        records = df.to_dict(orient="records")
        total = 0
        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i : i + BATCH_SIZE]
            total += self.publish_batch(chunk)
        return total

    @property
    def events_sent(self) -> int:
        """Total events published in this session."""
        return self._events_sent


# ---------------------------------------------------------------------------
# Async Publisher (higher throughput)
# ---------------------------------------------------------------------------
class AsyncSensorEventHubPublisher:
    """Asynchronous version of the Event Hub publisher for high-throughput scenarios."""

    def __init__(
        self,
        connection_string: Optional[str] = None,
        eventhub_name: str = EVENT_HUB_NAME,
    ) -> None:
        conn_str = connection_string or EVENT_HUB_CONNECTION_STR
        if not conn_str:
            raise ValueError(
                "EVENT_HUB_CONNECTION_STR environment variable is required"
            )
        self.conn_str = conn_str
        self.eventhub_name = eventhub_name

    async def publish_stream(
        self,
        generator: SensorDataGenerator,
        interval_seconds: float = 1.0,
        max_events: Optional[int] = None,
    ) -> int:
        """Continuously publish generated sensor events.

        Args:
            generator: SensorDataGenerator instance.
            interval_seconds: Delay between individual readings.
            max_events: Stop after this many events (None = run forever).

        Returns:
            Total events published.
        """
        total = 0
        async with AsyncEventHubProducerClient.from_connection_string(
            conn_str=self.conn_str, eventhub_name=self.eventhub_name
        ) as producer:
            while max_events is None or total < max_events:
                event = generator.generate_reading()
                batch = await producer.create_batch()
                batch.add(EventData(json.dumps(event)))
                await producer.send_batch(batch)
                total += 1
                if total % 100 == 0:
                    logger.info("Async publisher: %d events sent", total)
                await asyncio.sleep(interval_seconds)
        return total


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Machine Sensor Event Hub Publisher",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["simulate", "file", "async"],
        default="simulate",
        help="Publishing mode",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between sensor readings (simulate/async modes)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=BATCH_SIZE,
        help="Batch size for bulk publishing",
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to CSV file with sensor readings (file mode)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N events (default: run forever)",
    )
    parser.add_argument(
        "--machines",
        type=int,
        default=5,
        help="Number of simulated machines",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=ANOMALY_RATE,
        help="Fraction of readings that are anomalous",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point."""
    args = _parse_args()
    machine_ids = [f"MACHINE_{i:03d}" for i in range(1, args.machines + 1)]
    generator = SensorDataGenerator(
        machine_ids=machine_ids, anomaly_rate=args.anomaly_rate
    )
    publisher = SensorEventHubPublisher()

    if args.mode == "file":
        if not args.file:
            raise ValueError("--file is required in file mode")
        df = pd.read_csv(args.file)
        logger.info("Publishing %d rows from %s", len(df), args.file)
        total = publisher.publish_from_dataframe(df)
        logger.info("Done. %d events published.", total)

    elif args.mode == "async":
        async_publisher = AsyncSensorEventHubPublisher()
        asyncio.run(
            async_publisher.publish_stream(
                generator,
                interval_seconds=args.interval,
                max_events=args.max_events,
            )
        )

    else:  # simulate (synchronous)
        count = 0
        try:
            while args.max_events is None or count < args.max_events:
                batch = generator.generate_batch(args.batch)
                publisher.publish_batch(batch)
                count += len(batch)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Stopped. Total events published: %d", publisher.events_sent)


if __name__ == "__main__":
    main()
