"""
REST API - Flask service for real-time machine sensor anomaly detection.

Exposes HTTP endpoints to:
  - Accept raw sensor readings
  - Run trained Isolation Forest model inference
  - Return anomaly classification with confidence scores
  - Provide health, metrics, and model info endpoints

Deployment targets:
  - Azure Container Instances (ACI)
  - Azure Functions (with WSGI adapter)
  - Azure Container Apps

Environment Variables:
    MODEL_PATH      : Path to pickled model file (default: models/model_latest.pkl)
    API_KEY         : Optional API key for request authentication
    PORT            : HTTP port (default: 8000)
    LOG_LEVEL       : Logging level (default: INFO)
"""

import os
import pickle
import logging
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, abort, Response

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "models/model_latest.pkl")
API_KEY = os.getenv("API_KEY", "")  # Empty = no auth required
PORT = int(os.getenv("PORT", "8000"))

FEATURE_COLUMNS = [
    "temperature",
    "vibration",
    "pressure",
    "humidity",
    "current",
    "voltage",
]

# ---------------------------------------------------------------------------
# Model Loader
# ---------------------------------------------------------------------------
_model_artefact: Optional[Dict[str, Any]] = None


def load_model(path: str = MODEL_PATH) -> Dict[str, Any]:
    """Load a pickled model artefact from disk.

    Args:
        path: Path to the ``.pkl`` file produced by ``automl_trainer.py``.

    Returns:
        Artefact dictionary with keys ``model``, ``scaler``, ``params``,
        ``metrics``, ``trained_at``, and ``feature_columns``.

    Raises:
        FileNotFoundError: If the model file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model file not found: {path}")
    with open(path, "rb") as fh:
        artefact = pickle.load(fh)
    logger.info(
        "Model loaded from '%s' (trained_at=%s)", path, artefact.get("trained_at", "?")
    )
    return artefact


def get_model() -> Dict[str, Any]:
    """Return the cached model artefact, loading it on first call.

    Returns:
        Model artefact dictionary.
    """
    global _model_artefact  # noqa: PLW0603
    if _model_artefact is None:
        _model_artefact = load_model()
    return _model_artefact


# ---------------------------------------------------------------------------
# Prediction Logic
# ---------------------------------------------------------------------------
def predict_anomaly(
    readings: List[Dict[str, float]],
) -> List[Dict[str, Any]]:
    """Run anomaly detection on a list of sensor readings.

    Args:
        readings: List of dictionaries, each containing sensor values.

    Returns:
        List of prediction dictionaries with fields:
          - ``is_anomaly`` (bool)
          - ``anomaly_score`` (float, higher = more anomalous)
          - ``confidence`` (float 0–1)
          - ``features_used`` (list of str)
    """
    artefact = get_model()
    model = artefact["model"]
    scaler = artefact["scaler"]
    feature_cols: List[str] = artefact.get("feature_columns", FEATURE_COLUMNS)

    # Build feature matrix
    rows = []
    for r in readings:
        row = [float(r.get(col, 0.0)) for col in feature_cols]
        rows.append(row)

    X = np.array(rows)
    X_scaled = scaler.transform(X)

    raw_preds = model.predict(X_scaled)        # 1 = normal, -1 = anomaly
    scores = model.score_samples(X_scaled)     # lower = more anomalous

    # Normalise score to 0-1 anomaly probability (approximation)
    min_s, max_s = scores.min(), scores.max()
    score_range = max_s - min_s if max_s != min_s else 1e-9
    confidence = 1.0 - (scores - min_s) / score_range  # higher = more anomalous

    results = []
    for pred, score, conf in zip(raw_preds, scores, confidence):
        results.append(
            {
                "is_anomaly": bool(pred == -1),
                "anomaly_score": round(float(-score), 6),
                "confidence": round(float(conf), 4),
                "features_used": feature_cols,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Flask Application
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Request counter for /metrics endpoint
_request_counts: Dict[str, int] = {"total": 0, "anomalies": 0, "errors": 0}
_start_time = time.time()


def _check_api_key() -> None:
    """Validate API key header if API_KEY is configured.

    Raises:
        HTTPException 401 if key is missing or invalid.
    """
    if API_KEY:
        provided = request.headers.get("X-API-Key", "")
        if provided != API_KEY:
            logger.warning("Unauthorised request from %s", request.remote_addr)
            abort(401, description="Invalid or missing API key")


@app.before_request
def _before() -> None:
    """Increment request counter before each request."""
    _request_counts["total"] += 1


@app.errorhandler(400)
@app.errorhandler(401)
@app.errorhandler(404)
@app.errorhandler(422)
@app.errorhandler(500)
def _error_handler(exc) -> Tuple[Response, int]:
    """Return JSON error responses for all HTTP errors."""
    code = exc.code if hasattr(exc, "code") else 500
    msg = exc.description if hasattr(exc, "description") else str(exc)
    _request_counts["errors"] += 1
    return jsonify({"error": msg, "status": code}), code


# ------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------
@app.route("/", methods=["GET"])
def index() -> Response:
    """Root endpoint – service info."""
    return jsonify(
        {
            "service": "Machine Sensor Anomaly Detection API",
            "version": "1.0.0",
            "status": "running",
            "endpoints": [
                "GET  /health",
                "GET  /model/info",
                "POST /predict",
                "POST /predict/batch",
                "GET  /metrics",
            ],
        }
    )


@app.route("/health", methods=["GET"])
def health() -> Response:
    """Health check endpoint used by ACI / load balancers."""
    try:
        get_model()
        return jsonify({"status": "healthy", "model_loaded": True}), 200
    except FileNotFoundError:
        return jsonify({"status": "degraded", "model_loaded": False, "error": "Model file not found"}), 503


@app.route("/model/info", methods=["GET"])
def model_info() -> Response:
    """Return metadata about the currently loaded model."""
    _check_api_key()
    try:
        artefact = get_model()
        return jsonify(
            {
                "trained_at": artefact.get("trained_at"),
                "params": artefact.get("params"),
                "metrics": artefact.get("metrics"),
                "feature_columns": artefact.get("feature_columns"),
            }
        )
    except FileNotFoundError:
        abort(503, description="Model not available. Train model first.")


@app.route("/predict", methods=["POST"])
def predict_single() -> Response:
    """Predict anomaly for a single sensor reading.

    Request body (JSON):
        {
            "machine_id": "MACHINE_001",
            "temperature": 85.0,
            "vibration": 0.45,
            "pressure": 102.1,
            "humidity": 48.5,
            "current": 12.3,
            "voltage": 220.5
        }

    Response:
        {
            "machine_id": "MACHINE_001",
            "timestamp": "2026-04-10T03:00:00+00:00",
            "is_anomaly": false,
            "anomaly_score": 0.12345,
            "confidence": 0.23,
            "features_used": [...]
        }
    """
    _check_api_key()
    payload = request.get_json(silent=True)
    if not payload:
        abort(400, description="Request body must be valid JSON")

    try:
        results = predict_anomaly([payload])
        result = results[0]
    except Exception as exc:  # noqa: BLE001
        logger.exception("Prediction error: %s", exc)
        _request_counts["errors"] += 1
        abort(500, description="Prediction failed. Check server logs for details.")

    if result["is_anomaly"]:
        _request_counts["anomalies"] += 1

    response = {
        "machine_id": payload.get("machine_id", "unknown"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    logger.info(
        "Predict | machine=%s | anomaly=%s | score=%.4f",
        response["machine_id"],
        response["is_anomaly"],
        response["anomaly_score"],
    )
    return jsonify(response)


@app.route("/predict/batch", methods=["POST"])
def predict_batch() -> Response:
    """Predict anomalies for multiple sensor readings in one request.

    Request body (JSON):
        {
            "readings": [
                {"machine_id": "M1", "temperature": 80, ...},
                {"machine_id": "M2", "temperature": 150, ...}
            ]
        }

    Response:
        {
            "results": [
                {"machine_id": "M1", "is_anomaly": false, ...},
                {"machine_id": "M2", "is_anomaly": true, ...}
            ],
            "total": 2,
            "anomaly_count": 1
        }
    """
    _check_api_key()
    payload = request.get_json(silent=True)
    if not payload or "readings" not in payload:
        abort(400, description="Request body must contain 'readings' array")

    readings = payload["readings"]
    if not isinstance(readings, list) or len(readings) == 0:
        abort(422, description="'readings' must be a non-empty array")
    if len(readings) > 1000:
        abort(422, description="Maximum batch size is 1000 readings")

    try:
        predictions = predict_anomaly(readings)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Batch prediction error: %s", exc)
        _request_counts["errors"] += 1
        abort(500, description="Batch prediction failed. Check server logs for details.")

    results = []
    anomaly_count = 0
    for reading, pred in zip(readings, predictions):
        if pred["is_anomaly"]:
            anomaly_count += 1
            _request_counts["anomalies"] += 1
        results.append(
            {
                "machine_id": reading.get("machine_id", "unknown"),
                "timestamp": reading.get("timestamp", datetime.now(timezone.utc).isoformat()),
                **pred,
            }
        )

    logger.info(
        "Batch predict | count=%d | anomalies=%d", len(readings), anomaly_count
    )
    return jsonify(
        {
            "results": results,
            "total": len(results),
            "anomaly_count": anomaly_count,
        }
    )


@app.route("/metrics", methods=["GET"])
def metrics() -> Response:
    """Expose simple operational metrics."""
    uptime = round(time.time() - _start_time, 1)
    return jsonify(
        {
            "uptime_seconds": uptime,
            "requests_total": _request_counts["total"],
            "anomalies_detected": _request_counts["anomalies"],
            "errors_total": _request_counts["errors"],
        }
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Pre-load model at startup to fail fast if artefact is missing
    try:
        get_model()
    except FileNotFoundError as exc:
        logger.warning("Model not found at startup: %s. Train model first.", exc)

    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    logger.info("Starting REST API on port %d (debug=%s)", PORT, debug)
    app.run(host="0.0.0.0", port=PORT, debug=debug)
