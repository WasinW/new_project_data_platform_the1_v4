from setuptools import setup, find_packages

setup(
    name="dataflow_common_v2",
    version="2.0.0",  # Major version bump for refactored architecture
    # Explicitly list all packages (main + sub-packages)
    # REMOVED: connectors, transforms, utils (no longer needed)
    packages=[
        'dataflow_common_v2',
        'dataflow_common_v2.steps',
        'dataflow_common_v2.dofns',
    ],
    package_dir={'': '.'},
    python_requires=">=3.10",
    # TESTED COMPATIBLE SET - Must match Dockerfile versions!
    # s3fs → aiobotocore → botocore chain has STRICT version requirements
    install_requires=[
        "apache-beam[gcp]==2.69.0",  # MUST match Dockerfile SDK version
        "google-cloud-bigquery==3.25.0",
        "google-cloud-bigtable>=2.26.0",
        "google-cloud-pubsub>=2.23.1",
        "pyarrow==14.0.2",           # Pin to avoid conflicts
        "pandas>=1.5.0",
        # S3 dependencies - versions MUST be compatible!
        "fsspec==2024.6.1",
        "aiobotocore==2.13.0",
        "boto3==1.34.106",
        "botocore==1.34.106",
        "pyyaml>=6.0",
        "fastavro>=1.9.0",
    ],
    # Include all Python files
    include_package_data=False,
    zip_safe=False,
)
