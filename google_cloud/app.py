"""
Cloud Run FastAPI Service – Sensor Anomaly Detection API.

This service accepts sensor readings via HTTP POST, forwards them to a
Vertex AI endpoint for Isolation Forest scoring, and returns structured
anomaly detection results.  It is designed to be deployed on Cloud Run.

Endpoints:
    GET  /health             – Liveness and readiness probe
    POST /predict            – Predict anomaly for a single sensor reading
    POST /predict/batch      – Predict anomalies for a list of readings

Environment Variables:
    PROJECT_ID          – GCP project ID
    REGION              – Vertex AI endpoint region (default: us-central1)
    ENDPOINT_ID         – Vertex AI endpoint numeric ID
    LOG_LEVEL           – Python logging level (default: INFO)
    PORT                – Port to listen on (default: 8080)

Build & run locally:
    export PROJECT_ID=your-project-id
    export ENDPOINT_ID=1234567890
    uvicorn app:app --host 0.0.0.0 --port 8080 --reload
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import aiplatform
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID", "your-project-id")
REGION = os.environ.get("REGION", "us-central1")
ENDPOINT_ID = os.environ.get("ENDPOINT_ID", "")  # Numeric endpoint ID
SERVICE_VERSION = "1.0.0"

# Isolation Forest returns +1 (normal) or -1 (anomaly); some custom containers
# may return the raw anomaly score instead.  Set SCORE_MODE=score to use the
# raw score path, where values < ANOMALY_THRESHOLD are treated as anomalies.
SCORE_MODE = os.environ.get("SCORE_MODE", "label")  # "label" | "score"
ANOMALY_THRESHOLD = float(os.environ.get("ANOMALY_THRESHOLD", "0.0"))

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class SensorReading(BaseModel):
    """A single sensor reading submitted for anomaly detection."""

    sensor_id: str = Field(default="unknown", description="Unique sensor / machine identifier")
    temperature: float = Field(..., ge=-50.0, le=200.0, description="Temperature in °C")
    vibration: float = Field(..., ge=0.0, le=50.0, description="Vibration amplitude (mm/s)")
    pressure: float = Field(..., ge=500.0, le=1500.0, description="Pressure in hPa")
    value: float | None = Field(None, description="Generic sensor value (optional)")

    @field_validator("temperature", "vibration", "pressure")
    @classmethod
    def not_nan(cls, v: float) -> float:
        import math
        if math.isnan(v) or math.isinf(v):
            raise ValueError("Field must be a finite number.")
        return v


class PredictionResult(BaseModel):
    """Anomaly detection result for a single sensor reading."""

    sensor_id: str
    is_anomaly: bool
    anomaly_score: float | None = None
    confidence: str  # "high" | "medium" | "low"
    prediction_label: int  # 1 = normal, -1 = anomaly (Isolation Forest convention)
    latency_ms: float
    model_endpoint: str
    service_version: str


class BatchPredictionRequest(BaseModel):
    """A batch of sensor readings to evaluate in a single call."""

    readings: list[SensorReading] = Field(..., min_length=1, max_length=100)


class BatchPredictionResult(BaseModel):
    results: list[PredictionResult]
    total_anomalies: int
    total_readings: int
    latency_ms: float


# ---------------------------------------------------------------------------
# Vertex AI endpoint singleton
# ---------------------------------------------------------------------------

_endpoint: aiplatform.Endpoint | None = None


def get_endpoint() -> aiplatform.Endpoint:
    """Return the Vertex AI endpoint, initialising it on first call.

    Raises:
        HTTPException: If ENDPOINT_ID is not configured.
    """
    global _endpoint  # noqa: PLW0603
    if _endpoint is None:
        if not ENDPOINT_ID:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ENDPOINT_ID environment variable is not set.",
            )
        aiplatform.init(project=PROJECT_ID, location=REGION)
        _endpoint = aiplatform.Endpoint(
            endpoint_name=f"projects/{PROJECT_ID}/locations/{REGION}/endpoints/{ENDPOINT_ID}"
        )
        logger.info("Vertex AI endpoint initialised: %s", _endpoint.resource_name)
    return _endpoint


# ---------------------------------------------------------------------------
# Prediction logic
# ---------------------------------------------------------------------------


def _reading_to_instance(reading: SensorReading) -> dict[str, Any]:
    """Convert a :class:`SensorReading` to a Vertex AI prediction instance dict."""
    return {
        "temperature": reading.temperature,
        "vibration": reading.vibration,
        "pressure": reading.pressure,
        "value": reading.value if reading.value is not None else reading.temperature,
    }


def _parse_prediction(raw_prediction: Any, sensor_id: str, latency_ms: float) -> PredictionResult:
    """Interpret a raw Vertex AI prediction value into a :class:`PredictionResult`.

    The custom prediction container returns either:
    - An integer label: ``1`` (normal) or ``-1`` (anomaly)
    - A float anomaly score (lower = more normal)
    """
    endpoint_name = f"projects/{PROJECT_ID}/locations/{REGION}/endpoints/{ENDPOINT_ID}"

    if SCORE_MODE == "score":
        score = float(raw_prediction)
        is_anomaly = score < ANOMALY_THRESHOLD
        label = -1 if is_anomaly else 1
        confidence = "high" if abs(score - ANOMALY_THRESHOLD) > 0.3 else "low"
    else:
        label = int(raw_prediction)
        is_anomaly = label == -1
        score = None
        confidence = "high"

    return PredictionResult(
        sensor_id=sensor_id,
        is_anomaly=is_anomaly,
        anomaly_score=score,
        confidence=confidence,
        prediction_label=label,
        latency_ms=latency_ms,
        model_endpoint=endpoint_name,
        service_version=SERVICE_VERSION,
    )


async def predict_single(reading: SensorReading) -> PredictionResult:
    """Send one reading to Vertex AI and return the anomaly result.

    Args:
        reading: Validated sensor reading.

    Returns:
        :class:`PredictionResult` with anomaly flag and metadata.

    Raises:
        HTTPException: On Vertex AI API errors.
    """
    endpoint = get_endpoint()
    instance = _reading_to_instance(reading)

    t0 = time.perf_counter()
    try:
        response = endpoint.predict(instances=[instance])
    except GoogleAPICallError as exc:
        logger.error("Vertex AI prediction failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Prediction service error: {exc.message}",
        ) from exc
    latency_ms = (time.perf_counter() - t0) * 1000

    raw = response.predictions[0]
    return _parse_prediction(raw, reading.sensor_id, latency_ms)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise the Vertex AI endpoint on startup."""
    logger.info("Sensor Anomaly Detection API v%s starting …", SERVICE_VERSION)
    if ENDPOINT_ID:
        try:
            get_endpoint()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not pre-initialise Vertex AI endpoint: %s", exc)
    else:
        logger.warning("ENDPOINT_ID not set – /predict will fail until it is configured.")
    yield
    logger.info("API shutting down.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Machine Sensor Anomaly Detection API",
    description=(
        "Real-time anomaly detection for industrial machine sensors using "
        "Isolation Forest deployed on Vertex AI."
    ),
    version=SERVICE_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception for %s %s: %s", request.method, request.url, exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An internal server error occurred."},
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    summary="Health check",
    description="Liveness and readiness probe used by Cloud Run.",
    tags=["Operations"],
)
async def health_check():
    """Return service health status.

    Cloud Run uses this endpoint as a readiness / liveness probe.
    """
    return {
        "status": "healthy",
        "version": SERVICE_VERSION,
        "project": PROJECT_ID,
        "region": REGION,
        "endpoint_configured": bool(ENDPOINT_ID),
    }


@app.post(
    "/predict",
    response_model=PredictionResult,
    summary="Predict anomaly for a single sensor reading",
    tags=["Prediction"],
)
async def predict(reading: SensorReading):
    """Evaluate one sensor reading for anomalies.

    Sends the reading to the Vertex AI Isolation Forest endpoint and returns
    a structured result indicating whether an anomaly was detected.

    Example request body:
    ```json
    {
        "sensor_id": "machine-001",
        "temperature": 25.5,
        "vibration": 0.12,
        "pressure": 1013.25,
        "value": 25.5
    }
    ```
    """
    logger.info("Prediction request: sensor_id=%s", reading.sensor_id)
    result = await predict_single(reading)
    if result.is_anomaly:
        logger.warning(
            "ANOMALY detected | sensor_id=%s score=%s",
            reading.sensor_id,
            result.anomaly_score,
        )
    return result


@app.post(
    "/predict/batch",
    response_model=BatchPredictionResult,
    summary="Predict anomalies for a batch of sensor readings",
    tags=["Prediction"],
)
async def predict_batch(batch: BatchPredictionRequest):
    """Evaluate up to 100 sensor readings for anomalies in a single call.

    Each reading is sent to Vertex AI individually; results are aggregated
    and returned together.
    """
    t0 = time.perf_counter()
    results = []
    for reading in batch.readings:
        result = await predict_single(reading)
        results.append(result)

    total_ms = (time.perf_counter() - t0) * 1000
    anomaly_count = sum(1 for r in results if r.is_anomaly)

    logger.info(
        "Batch prediction complete | %d readings | %d anomalies | %.1f ms",
        len(results), anomaly_count, total_ms,
    )

    return BatchPredictionResult(
        results=results,
        total_anomalies=anomaly_count,
        total_readings=len(results),
        latency_ms=total_ms,
    )


# ---------------------------------------------------------------------------
# Development entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
