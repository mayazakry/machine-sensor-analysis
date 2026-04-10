"""
Vertex AI Model Deployment Script.

This script registers the trained Isolation Forest model stored in GCS with
the Vertex AI Model Registry and deploys it to a Vertex AI Endpoint for
real-time online prediction.

Prerequisites:
    1. A trained model artifact exists at GCS_MODEL_DIR (model.joblib +
       scaler.joblib produced by train_vertex_ai.py).
    2. A custom prediction container image is built and pushed to Artifact
       Registry (see Dockerfile.predictor in the same directory).

Usage:
    python deploy_vertex_ai.py \
        --project YOUR_PROJECT_ID \
        --region us-central1 \
        --model-dir gs://your-bucket/models/ \
        --endpoint-name sensor-anomaly-endpoint \
        --image gcr.io/YOUR_PROJECT/sensor-predictor:latest

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS  – Service account key path
    PROJECT_ID                       – GCP project ID
    REGION                           – GCP region (default: us-central1)
    MODEL_DIR                        – GCS URI of the model artifact directory
    ENDPOINT_NAME                    – Display name for the Vertex AI endpoint
    PREDICTION_IMAGE                 – URI of the custom prediction container
"""

import argparse
import logging
import os
import sys
import time

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud import aiplatform

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
# Defaults (overridden by CLI args / env vars)
# ---------------------------------------------------------------------------
DEFAULT_PROJECT = os.environ.get("PROJECT_ID", "your-project-id")
DEFAULT_REGION = os.environ.get("REGION", "us-central1")
DEFAULT_MODEL_DIR = os.environ.get("MODEL_DIR", "gs://your-bucket/models/")
DEFAULT_ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "sensor-anomaly-endpoint")
DEFAULT_IMAGE = os.environ.get(
    "PREDICTION_IMAGE",
    "us-central1-docker.pkg.dev/your-project/sensor-repo/predictor:latest",
)
DEFAULT_MACHINE_TYPE = os.environ.get("MACHINE_TYPE", "n1-standard-2")
DEFAULT_MIN_REPLICAS = int(os.environ.get("MIN_REPLICAS", "1"))
DEFAULT_MAX_REPLICAS = int(os.environ.get("MAX_REPLICAS", "3"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_or_create_endpoint(
    display_name: str,
    project: str,
    location: str,
) -> aiplatform.Endpoint:
    """Return an existing endpoint with *display_name*, or create a new one.

    Args:
        display_name: Human-readable endpoint name.
        project: GCP project ID.
        location: GCP region.

    Returns:
        A :class:`aiplatform.Endpoint` instance.
    """
    endpoints = aiplatform.Endpoint.list(
        filter=f'display_name="{display_name}"',
        project=project,
        location=location,
    )
    if endpoints:
        endpoint = endpoints[0]
        logger.info("Reusing existing endpoint: %s (%s)", display_name, endpoint.resource_name)
        return endpoint

    logger.info("Creating new endpoint: %s", display_name)
    endpoint = aiplatform.Endpoint.create(
        display_name=display_name,
        project=project,
        location=location,
    )
    logger.info("Endpoint created: %s", endpoint.resource_name)
    return endpoint


def upload_model(
    display_name: str,
    model_dir: str,
    serving_container_image_uri: str,
    project: str,
    location: str,
) -> aiplatform.Model:
    """Upload the model artifact from GCS to Vertex AI Model Registry.

    Args:
        display_name: Human-readable model display name.
        model_dir: GCS URI of the directory containing model artifacts.
        serving_container_image_uri: URI of the Docker image used for serving.
        project: GCP project ID.
        location: GCP region.

    Returns:
        A :class:`aiplatform.Model` resource.
    """
    logger.info("Uploading model from %s", model_dir)

    model = aiplatform.Model.upload(
        display_name=display_name,
        artifact_uri=model_dir,
        serving_container_image_uri=serving_container_image_uri,
        serving_container_predict_route="/predict",
        serving_container_health_route="/health",
        serving_container_ports=[8080],
        # Environment variables injected into the serving container
        serving_container_environment_variables={
            "MODEL_DIR": "/tmp/model",
            "LOG_LEVEL": "INFO",
        },
        project=project,
        location=location,
    )
    logger.info("Model uploaded: %s", model.resource_name)
    return model


def deploy_model(
    model: aiplatform.Model,
    endpoint: aiplatform.Endpoint,
    machine_type: str,
    min_replicas: int,
    max_replicas: int,
) -> None:
    """Deploy *model* to *endpoint* with auto-scaling configuration.

    Args:
        model: Vertex AI Model resource to deploy.
        endpoint: Vertex AI Endpoint to deploy the model to.
        machine_type: Compute Engine machine type for the serving replicas.
        min_replicas: Minimum number of serving replicas.
        max_replicas: Maximum number of serving replicas.
    """
    logger.info(
        "Deploying model %s to endpoint %s | machine=%s | replicas=[%d, %d]",
        model.display_name,
        endpoint.display_name,
        machine_type,
        min_replicas,
        max_replicas,
    )

    model.deploy(
        endpoint=endpoint,
        deployed_model_display_name=model.display_name,
        machine_type=machine_type,
        min_replica_count=min_replicas,
        max_replica_count=max_replicas,
        traffic_split={"0": 100},
        sync=True,
    )
    logger.info("Model deployed successfully.")


def smoke_test_endpoint(endpoint: aiplatform.Endpoint) -> None:
    """Send a synthetic sensor reading to verify the endpoint is alive.

    A prediction score of ``-1`` from the model indicates an anomaly.

    Args:
        endpoint: Deployed :class:`aiplatform.Endpoint`.
    """
    test_instance = {
        "temperature": 25.5,
        "vibration": 0.12,
        "pressure": 1013.25,
        "value": 25.5,
    }
    logger.info("Sending smoke-test prediction: %s", test_instance)

    try:
        response = endpoint.predict(instances=[test_instance])
        logger.info("Smoke-test response: %s", response.predictions)
    except GoogleAPICallError as exc:
        logger.warning("Smoke-test prediction failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register and deploy a trained model to a Vertex AI endpoint."
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    parser.add_argument("--region", default=DEFAULT_REGION, help="GCP region")
    parser.add_argument("--model-dir", default=DEFAULT_MODEL_DIR, help="GCS model artifact URI")
    parser.add_argument("--endpoint-name", default=DEFAULT_ENDPOINT_NAME, help="Endpoint display name")
    parser.add_argument("--image", default=DEFAULT_IMAGE, help="Serving container image URI")
    parser.add_argument("--machine-type", default=DEFAULT_MACHINE_TYPE, help="Machine type")
    parser.add_argument("--min-replicas", type=int, default=DEFAULT_MIN_REPLICAS)
    parser.add_argument("--max-replicas", type=int, default=DEFAULT_MAX_REPLICAS)
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=True,
        help="Send a test prediction after deployment (default: True)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    logger.info("=== Vertex AI Deployment Starting ===")
    logger.info("Project      : %s", args.project)
    logger.info("Region       : %s", args.region)
    logger.info("Model dir    : %s", args.model_dir)
    logger.info("Endpoint     : %s", args.endpoint_name)
    logger.info("Image        : %s", args.image)

    aiplatform.init(project=args.project, location=args.region)

    # 1. Get or create the endpoint
    endpoint = get_or_create_endpoint(args.endpoint_name, args.project, args.region)

    # 2. Upload the model to Model Registry
    model = upload_model(
        display_name="sensor-anomaly-detector",
        model_dir=args.model_dir,
        serving_container_image_uri=args.image,
        project=args.project,
        location=args.region,
    )

    # 3. Deploy model to endpoint
    deploy_model(
        model=model,
        endpoint=endpoint,
        machine_type=args.machine_type,
        min_replicas=args.min_replicas,
        max_replicas=args.max_replicas,
    )

    # 4. Smoke test
    if args.smoke_test:
        # Allow a brief warm-up before testing
        logger.info("Waiting 30 s for endpoint warm-up …")
        time.sleep(30)
        smoke_test_endpoint(endpoint)

    logger.info("=== Deployment Complete ===")
    logger.info("Endpoint resource name: %s", endpoint.resource_name)


if __name__ == "__main__":
    main()
