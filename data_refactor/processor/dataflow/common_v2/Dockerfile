# ================================
# Dataflow Common v2.0.0 - Refactored
# ================================
# TESTED COMPATIBLE SET - All versions must match across driver and worker!
FROM apache/beam_python3.11_sdk:2.69.0

ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1
RUN echo "========== ENVIRONMENT INFO ==========" && \
    python --version && \
    pip --version && \
    echo "========================================"

# ============================================
# STEP 1: Install numpy<2 FIRST (CRITICAL)
# pyarrow 14.0.2 was compiled with NumPy 1.x
# ============================================
RUN echo "========== STEP 1: Installing numpy ==========" && \
    pip install --no-cache-dir 'numpy>=1.21,<2' && \
    python -c "import numpy; print(f'numpy {numpy.__version__} installed')" && \
    echo "================================================"

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
    python -c "import pyarrow; print(f'pyarrow {pyarrow.__version__}')" && \
    python -c "from google.cloud import bigquery; print('google-cloud-bigquery OK')" && \
    python -c "from google.cloud import bigtable; print('google-cloud-bigtable OK')" && \
    python -c "import yaml; print(f'pyyaml {yaml.__version__}')" && \
    python -c "import fastavro; print(f'fastavro {fastavro.__version__}')" && \
    python -c "import pandas; print(f'pandas {pandas.__version__}')" && \
    echo "======================================================"

# ============================================
# STEP 3: Install S3 dependencies
# STRICT version requirements for s3fs chain
# ============================================
RUN echo "========== STEP 3: Installing S3 deps ==========" && \
    pip install --no-cache-dir \
        boto3==1.34.106 \
        botocore==1.34.106 \
        aiobotocore==2.13.0 \
        s3fs==2024.6.1 \
        fsspec==2024.6.1 && \
    echo "--- Verifying S3 deps ---" && \
    python -c "import boto3; print(f'boto3 {boto3.__version__}')" && \
    python -c "import botocore; print(f'botocore {botocore.__version__}')" && \
    python -c "import aiobotocore; print(f'aiobotocore {aiobotocore.__version__}')" && \
    python -c "import s3fs; print(f's3fs {s3fs.__version__}')" && \
    python -c "import fsspec; print(f'fsspec {fsspec.__version__}')" && \
    echo "=================================================="

# ============================================
# STEP 4: Install dataflow_common_v2 wheel
# ============================================
COPY dist/dataflow_common_v2-2.0.0-py3-none-any.whl /tmp/

RUN echo "========== STEP 4: Installing dataflow_common_v2 ==========" && \
    pip install --no-cache-dir --no-deps /tmp/dataflow_common_v2-2.0.0-py3-none-any.whl && \
    rm /tmp/dataflow_common_v2-2.0.0-py3-none-any.whl && \
    echo "--- Verifying dataflow_common_v2 package structure ---" && \
    python -c "import dataflow_common_v2; print(f'dataflow_common_v2 {dataflow_common_v2.__version__}')" && \
    echo "========================================================"

# ============================================
# STEP 5: Comprehensive import verification
# This is CRITICAL - if any import fails here,
# the SDK harness will fail to register!
# ============================================
# Verify all imports - v2.0.0 uses PTransform pattern (no orchestrator/config)
RUN python -c "\
from dataflow_common_v2 import __version__; \
from dataflow_common_v2.steps import RefreshMappingTableTransform; \
from dataflow_common_v2.steps import ReadFromPubSubTransform; \
from dataflow_common_v2.steps import WriteToBigQueryCDCTransform; \
from dataflow_common_v2.dofns import MappingRefreshDoFn; \
from dataflow_common_v2.dofns import ExtractPersonasDoFn; \
from dataflow_common_v2.dofns import DLQOutputMixin; \
print(f'All dataflow_common_v2 v{__version__} imports successful'); \
"

WORKDIR /
