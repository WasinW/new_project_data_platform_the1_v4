from setuptools import setup, find_packages
import os

# setup(
#     name="dataflow_common",
#     version="1.0.0",
#     packages=find_packages(where=".."),
#     package_dir={"dataflow_common": "."},
#     python_requires=">=3.8",
#     install_requires=[
#         "pyyaml>=6.0",
#         "apache-beam>=2.69.0",
#         "google-cloud-bigquery>=3.25.0",
#         "pyarrow>=14.0.0",
#         "pytest==7.4.3",
#     ],
# )


setup(
    name="dataflow_common",
    version="1.0.0",
    # Explicitly list all packages (main + sub-packages)
    packages=[
        'dataflow_common',
        'dataflow_common.connectors',
        'dataflow_common.steps',
        'dataflow_common.dofns',
        # 'dataflow_common.steps.dofns',  # Added: DoFn classes subpackage
        'dataflow_common.transforms',
        'dataflow_common.utils',
    ],
    package_dir={'': '.'},
    # CRITICAL FIX: package_dir must be {'dataflow_common': '.'} NOT {'': '.'}
    # Wrong value causes wheel to not include packages correctly
    # which leads to "SDK harnesses are not healthy" errors on Dataflow workers
    # because workers cannot import dataflow_common modules
    # package_dir={'dataflow_common': '.'},
    python_requires=">=3.10",
    # TESTED COMPATIBLE SET - Must match Dockerfile versions!
    # s3fs → aiobotocore → botocore chain has STRICT version requirements
    # See: https://stackoverflow.com/questions/75743038
    install_requires=[
        "apache-beam[gcp]==2.69.0",  # MUST match Dockerfile SDK version
        "google-cloud-bigquery==3.25.0",
        "google-cloud-bigtable>=2.26.0",
        "google-cloud-pubsub>=2.23.1",
        "pyarrow==14.0.2",           # Pin to avoid conflicts
        # "numpy<2",                   # CRITICAL: pyarrow 14.x requires numpy 1.x
        "pandas>=1.5.0",
        # S3 dependencies - versions MUST be compatible!
        # s3fs==2024.6.1 → aiobotocore==2.13.0 → botocore>=1.34.70,<1.34.107
        # "s3fs==2024.6.1",
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

# # Get all Python modules in current directory
# modules = [f[:-3] for f in os.listdir('.') 
#            if f.endswith('.py') and f != 'setup.py']
# # List all Python files we want to include
# def get_package_files():
#     files = []
#     for root, dirs, filenames in os.walk('.'):
#         # Skip hidden dirs and build dirs
#         dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['build', 'dist', '__pycache__']]
#         for filename in filenames:
#             if filename.endswith('.py') and filename != 'setup.py':
#                 files.append(os.path.join(root, filename)[2:])  # Remove './'
#     return files

# setup(
#     name="dataflow_common",
#     version="1.0.0",
#     packages=['dataflow_common'],
#     package_dir={'dataflow_common': '.'},
#     py_modules=['dataflow_common.' + f[:-3].replace('/', '.') for f in get_package_files() if f.endswith('.py')],
#     package_data={
#         'dataflow_common': ['**/*.py']
#     },
#     include_package_data=True,
#     python_requires=">=3.8",
#     install_requires=[
#         "pyyaml>=6.0",
#         "apache-beam[gcp]>=2.69.0",
#         "google-cloud-bigquery>=3.25.0",
#         "google-cloud-bigtable>=2.26.0",
#         "google-cloud-pubsub>=2.23.1",
#         "pyarrow>=14.0.0",
#     ],
# )