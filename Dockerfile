# EU Energy Transition Analysis - Docker Image
# Python environment with dbt and ingestion scripts

FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY scripts/ ./scripts/
COPY dbt/ ./dbt/

# Create directory for GCP credentials (mounted at runtime)
RUN mkdir -p /secrets

# Set default environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-key.json \
    DBT_PROFILES_DIR=/app/dbt

# Default command: show help
CMD ["echo", "Usage: docker-compose run app python scripts/ingest_entsoe_data.py"]
