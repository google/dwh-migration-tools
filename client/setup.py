"""setup.py for dwh_migration_client."""
from setuptools import PEP420PackageFinder, setup

with open("README.md") as readme:
  long_description = readme.read()

setup(
    name="dwh_migration_client",
    description="Exemplary Python Client for the BigQuery SQL Translator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/google/dwh-migration-tools/client",
    version="0.1.0",
    python_requires=">=3.7",
    packages=PEP420PackageFinder.find(),
    namespace_packages=('google', 'google.cloud'),
    install_requires=[
        # google cloud packages.
        "google-cloud>=0.34.0",
        "google-cloud-storage>=2.3.0",
        "google-api-python-client>=2.45.0",
        # protobuf
        "protobuf>=3.20.1",
        # gRPC,
        "grpcio>=1.46.1",
        # Used to marshal config file data.
        "PyYAML>=6.0",
        "marshmallow<=3.14.1",
        'google-api-core[grpc] >= 2.8.0, < 3.0.0dev',
        'libcst >= 0.2.5',
        'googleapis-common-protos >= 1.55.0, <2.0.0dev',
        'proto-plus >= 1.19.7',
    ],
    extras_require={
        "dev": [
            "black",
            "isort",
            "pylint",
            "mypy",
            "pytest",
            "nox",
            "types-setuptools",
            "types-PyYAML",
            "types-protobuf",
            "types-requests",
        ]
    },
    entry_points={
        "console_scripts": [
            "dwh-migration-client=dwh_migration_client.main:main"
        ]
    },
)
