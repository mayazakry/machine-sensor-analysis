# =============================================================================
# Dockerfile – Machine Sensor Anomaly Detection REST API
# =============================================================================
# Multi-stage build:
#   Stage 1 (builder): Install dependencies into a virtual environment
#   Stage 2 (runtime): Copy only the venv – minimal final image
#
# Build:
#   docker build -t sensor-api:latest .
#
# Run locally:
#   docker run -p 8000:8000 \
#     -e MODEL_PATH=/app/models/model_latest.pkl \
#     -v $(pwd)/models:/app/models:ro \
#     sensor-api:latest
# =============================================================================

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Install system build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy requirements first for better layer caching
COPY requirements.txt .

# Create a virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip --no-cache-dir \
 && pip install --no-cache-dir -r requirements.txt


# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Security: run as non-root user
RUN groupadd --gid 1001 appgroup \
 && useradd --uid 1001 --gid appgroup --no-create-home appuser

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application source
COPY rest_api_app.py   ./rest_api_app.py
COPY automl_trainer.py ./automl_trainer.py
COPY config.yaml       ./config.yaml

# Create models directory (model file mounted at runtime or baked in)
RUN mkdir -p /app/models && chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# ── Environment Defaults ──────────────────────────────────────────────────────
ENV PORT=8000 \
    LOG_LEVEL=INFO \
    MODEL_PATH=/app/models/model_latest.pkl \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# ── Health Check ──────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

EXPOSE ${PORT}

# ── Entry Point ───────────────────────────────────────────────────────────────
# Use gunicorn for production WSGI serving
CMD ["sh", "-c", "gunicorn \
    --bind 0.0.0.0:${PORT} \
    --workers 2 \
    --timeout 30 \
    --access-logfile - \
    --error-logfile - \
    --log-level ${LOG_LEVEL} \
    rest_api_app:app"]
