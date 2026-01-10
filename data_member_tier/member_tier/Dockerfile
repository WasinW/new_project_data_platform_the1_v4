# =============================================================================
# Dockerfile for Member Tiers Kafka Consumer Pipeline
# Path: data-pipeline/processor/dataflow/scripts/member_tiers/Dockerfile
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder (install dependencies)
# -----------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies (without lock file)
RUN uv sync --no-dev

# -----------------------------------------------------------------------------
# Stage 2: Runtime (Beam SDK)
# -----------------------------------------------------------------------------
# FROM apache/beam_python3.11_sdk:2.61.0
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest


WORKDIR /app

# Install Java (REQUIRED for Kafka cross-language transform!)
# ReadFromKafka needs Java to start the expansion service during pipeline construction
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && java -version

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy pipeline script
COPY member_tiers.py /app/pipeline.py

# Flex Template configuration
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline.py"

# Entrypoint for Dataflow workers
# ENTRYPOINT ["/opt/apache/beam/boot"]