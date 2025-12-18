# FROM apache/beam_python3.11_sdk:2.59.0

# ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# # Install dependencies ตามลำดับที่ถูกต้อง
# RUN pip install --no-cache-dir --no-deps \
#     six==1.16.0 \
#     python-dateutil==2.9.0.post0 \
#     jmespath==1.0.1 \
#     urllib3==1.26.18

# RUN pip install --no-cache-dir --no-deps \
#     botocore==1.34.106 \
#     s3transfer==0.10.1

# RUN pip install --no-cache-dir --no-deps \
#     boto3==1.34.106

# RUN pip install --no-cache-dir \
#     pyarrow==14.0.2 \
#     google-cloud-bigquery==3.25.0 \
#     pyyaml==6.0.1

# # Install dataflow_common จาก wheel
# COPY dist/dataflow_common-1.0.0-py3-none-any.whl /tmp/
# RUN pip install --no-cache-dir /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
#     rm /tmp/dataflow_common-1.0.0-py3-none-any.whl

# # Verify
# RUN python -c "from dataflow_common.config import load_config; print('Import successful!')"

# WORKDIR /

# --------------------------------- VERSION FIX ---------------------------------
# TESTED COMPATIBLE SET - All versions must match across driver and worker!
# FROM apache/beam_python3.11_sdk:2.69.0
# FROM apache/beam_python3.10_sdk:2.69.0
FROM apache/beam_python3.11_sdk:2.69.0

# FROM --platform=linux/amd64 apache/beam_python3.11_sdk:2.59.0

ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1
RUN echo "========== ENVIRONMENT INFO ==========" && \
    python --version && \
    pip --version && \
    echo "========================================"

# ============================================
# STEP 1: Install numpy<2 FIRST (CRITICAL)
# pyarrow 14.0.2 was compiled with NumPy 1.x
# ============================================

# CRITICAL: Install numpy<2 FIRST before other packages
# pyarrow 14.0.2 was compiled with NumPy 1.x and will fail with NumPy 2.x
RUN echo "========== STEP 1: Installing numpy ==========" && \
    pip install --no-cache-dir 'numpy>=1.21,<2' && \
    python -c "import numpy; print(f'✅ numpy {numpy.__version__} installed')" && \
    echo "================================================"

# Install S3 dependencies with compatible versions
# s3fs → aiobotocore → botocore chain (aiobotocore pulls aiohttp automatically)
# ============================================
# STEP 2: Install core dependencies
# ============================================
RUN echo "========== STEP 2: Installing core deps ==========" && \
    pip install --no-cache-dir \
        pyarrow==14.0.2 \
        google-cloud-bigquery==3.25.0 \
        google-cloud-bigtable>=2.26.0 \
        pyyaml==6.0.1 \
        fastavro>=1.9.0 \
        'pandas>=1.5.0,<2.1.0' && \
    echo "--- Verifying core deps ---" && \
    python -c "import pyarrow; print(f'✅ pyarrow {pyarrow.__version__}')" && \
    python -c "from google.cloud import bigquery; print('✅ google-cloud-bigquery')" && \
    python -c "from google.cloud import bigtable; print('✅ google-cloud-bigtable')" && \
    python -c "import yaml; print(f'✅ pyyaml {yaml.__version__}')" && \
    python -c "import fastavro; print(f'✅ fastavro {fastavro.__version__}')" && \
    python -c "import pandas; print(f'✅ pandas {pandas.__version__}')" && \
    echo "======================================================"

# ============================================
# STEP 3: Install S3 dependencies
# STRICT version requirements for s3fs chain
# ============================================
RUN echo "========== STEP 3: Installing S3 deps =========="
# Install S3 dependencies with compatible versions
# s3fs → aiobotocore → botocore chain has STRICT version requirements
    RUN pip install --no-cache-dir \
        boto3==1.34.106 \
        botocore==1.34.106 \
        aiobotocore==2.13.0 \
        s3fs==2024.6.1 \
        fsspec==2024.6.1 && \
    echo "--- Verifying S3 deps ---" && \
    python -c "import boto3; print(f'✅ boto3 {boto3.__version__}')" && \
    python -c "import botocore; print(f'✅ botocore {botocore.__version__}')" && \
    python -c "import aiobotocore; print(f'✅ aiobotocore {aiobotocore.__version__}')" && \
    python -c "import s3fs; print(f'✅ s3fs {s3fs.__version__}')" && \
    python -c "import fsspec; print(f'✅ fsspec {fsspec.__version__}')" && \
    echo "=================================================="

# ============================================
# STEP 4: Install dataflow_common wheel
# ============================================

# Copy และ install wheel
COPY dist/dataflow_common-1.0.0-py3-none-any.whl /tmp/

# RUN pip install --no-cache-dir /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
RUN echo "========== STEP 4: Installing dataflow_common ==========" && \
    pip install --no-cache-dir --no-deps /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
    rm /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
    echo "--- Verifying dataflow_common package structure ---" && \
    python -c "import dataflow_common; print(f'✅ dataflow_common {dataflow_common.__version__}')" && \
    echo "========================================================"

# ============================================
# STEP 5: Comprehensive import verification
# This is CRITICAL - if any import fails here,
# the SDK harness will fail to register!
# ============================================

# Verify
RUN python -c "from dataflow_common.config import load_config; print('✅ Import OK')"
# Comprehensive verification - all these imports must succeed for SDK harness to register
# If any import fails, worker will crash with "SDK harnesses are not healthy" error
RUN python -c "\
from dataflow_common.config import load_config; \
from dataflow_common.orchestrator import Orchestrator; \
from dataflow_common.registry import STEP_REGISTRY; \
from dataflow_common.steps import ReadBQQueryStep, WriteParquetStep; \
print('✅ All dataflow_common imports successful'); \
print(f'✅ Registered steps: {len(STEP_REGISTRY)} steps available'); \
"

WORKDIR /