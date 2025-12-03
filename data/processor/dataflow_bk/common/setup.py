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
#         "apache-beam>=2.59.0",
#         "google-cloud-bigquery>=3.25.0",
#         "pyarrow>=14.0.0",
#         "pytest==7.4.3",
#     ],
# )


setup(
    name="dataflow_common",
    version="1.0.0",
    packages=['dataflow_common'],
    package_dir={'dataflow_common': '.'},
    package_data={
        'dataflow_common': [
            '*.py',
            'connectors/*.py',
            'steps/*.py', 
            'transforms/*.py',
            'utils/*.py'
        ]
    },
    python_requires=">=3.9",
    install_requires=[
        "apache-beam[gcp]==2.69.0",
        "google-cloud-bigquery==3.25.0",
        "pyarrow>=14.0.0",
        "pyyaml>=6.0",
        "boto3>=1.26.0",
    ],
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
#         "apache-beam[gcp]>=2.59.0",
#         "google-cloud-bigquery>=3.25.0",
#         "google-cloud-bigtable>=2.26.0",
#         "google-cloud-pubsub>=2.23.1",
#         "pyarrow>=14.0.0",
#     ],
# )