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
# CRITICAL: Install numpy<2 FIRST before other packages
# pyarrow 14.0.2 was compiled with NumPy 1.x and will fail with NumPy 2.x
RUN pip install --no-cache-dir 'numpy>=1.21,<2'

# Install S3 dependencies with compatible versions
# s3fs → aiobotocore → botocore chain (aiobotocore pulls aiohttp automatically)
RUN pip install --no-cache-dir \
    pyarrow==14.0.2 \
    google-cloud-bigquery==3.25.0 \
    google-cloud-bigtable>=2.26.0 \
    pyyaml==6.0.1 \
    fastavro>=1.9.0 \
    'pandas>=1.5.0,<2.1.0'

    # Install S3 dependencies with compatible versions
# s3fs → aiobotocore → botocore chain has STRICT version requirements
RUN pip install --no-cache-dir \
    boto3==1.34.106 \
    botocore==1.34.106 \
    aiobotocore==2.13.0 \
    s3fs==2024.6.1 \
    fsspec==2024.6.1

# Copy และ install wheel
COPY dist/dataflow_common-1.0.0-py3-none-any.whl /tmp/

# RUN pip install --no-cache-dir /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
RUN pip install --no-cache-dir --no-deps /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
    rm /tmp/dataflow_common-1.0.0-py3-none-any.whl

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