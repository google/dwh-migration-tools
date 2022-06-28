"""setup.py for dwh_migration_client."""
from setuptools import find_packages, setup

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
    packages=find_packages(),
    install_requires=[
        # google cloud packages.
        "google-cloud>=0.34.0",
        "google-cloud-storage>=2.3.0",
        "google-cloud-bigquery_migration>=0.5.0",
        "google-api-python-client>=2.45.0",
        # protobuf
        "protobuf>=3.20.1",
        # gRPC,
        "grpcio>=1.46.1",
        # PyYML package to parse yaml config files.
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "black",
            "isort",
            "pylint",
            "mypy",
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
