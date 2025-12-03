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
FROM apache/beam_python3.11_sdk:2.69.0

ENV RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1

# Install boto3 dependencies ตามลำดับ
RUN pip install --no-cache-dir --no-deps \
    six==1.16.0 \
    python-dateutil==2.9.0.post0 \
    jmespath==1.0.1 \
    urllib3==1.26.18 \
    botocore==1.34.106 \
    s3transfer==0.10.1 \
    boto3==1.34.106

# Install other dependencies
RUN pip install --no-cache-dir \
    google-cloud-bigquery==3.25.0 \
    aiobotocore>=2.5.0 \
    fsspec>=2023.1.0 \
    pyarrow==14.0.2 \
    s3fs>=2023.1.0 \
    pyyaml==6.0.1

# Copy และ install wheel
# COPY dist/dataflow_common-1.0.0-py3-none-any.whl /tmp/
# RUN pip install --no-cache-dir /tmp/dataflow_common-1.0.0-py3-none-any.whl && \
#     rm /tmp/dataflow_common-1.0.0-py3-none-any.whl

# Verify
# RUN python -c "from dataflow_common.config import load_config; print('✅ Import OK')"

WORKDIR /