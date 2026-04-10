# =============================================================================
# Dockerfile – Cloud Run sensor anomaly detection API
#
# Build:
#   docker build -t sensor-anomaly-api .
#
# Run locally:
#   docker run -p 8080:8080 \
#     -e PROJECT_ID=your-project-id \
#     -e ENDPOINT_ID=1234567890 \
#     sensor-anomaly-api
# =============================================================================

# ---------------------------------------------------------------------------
# Base image – slim Python 3.11 for smaller image size
# ---------------------------------------------------------------------------
FROM python:3.11-slim

# ---------------------------------------------------------------------------
# OS-level dependencies
# ---------------------------------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Create a non-root user for security
# ---------------------------------------------------------------------------
RUN useradd --create-home --shell /bin/bash appuser

# ---------------------------------------------------------------------------
# Working directory
# ---------------------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------------------
# Install Python dependencies
# Copying requirements first leverages Docker layer caching so that a code
# change does not force a full re-install of all packages.
# ---------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Copy application source code
# ---------------------------------------------------------------------------
COPY app.py .
COPY monitoring.py .

# ---------------------------------------------------------------------------
# Switch to non-root user
# ---------------------------------------------------------------------------
USER appuser

# ---------------------------------------------------------------------------
# Cloud Run injects PORT; default to 8080
# ---------------------------------------------------------------------------
ENV PORT=8080
EXPOSE 8080

# ---------------------------------------------------------------------------
# Health check (used by Cloud Run and Docker Desktop)
# ---------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:${PORT}/health || exit 1

# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $PORT --workers 2 --log-level info"]
