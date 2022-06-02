"""setup.py for dwh_migration_client."""
from setuptools import find_packages, setup


setup(
    name="dwh-migration-client",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.0",
    install_requires=[
        # google cloud packages.
        "google-cloud>=0.34.0",
        "google-cloud-storage>=2.3.0",
        "google-cloud-bigquery_migration>=0.4.0",
        "google-api-python-client>=2.45.0",
        # protobuf
        "protobuf>=3.20.1",
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
            "dwh-migration-client=dwh_migration_client.main:start_translation"
        ]
    },
)