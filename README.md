# Data Warehouse Migration Tools
[![DWH Dumper CI](https://github.com/google/dwh-migration-tools/actions/workflows/tests.yml/badge.svg)](https://github.com/google/dwh-migration-tools/actions/workflows/tests.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project contains a collection of tools related to the [BigQuery Migration
Service](https://cloud.google.com/bigquery/docs/migration-intro).

**[Download the latest cross-platform release zip `dwh-migration-tools-vX.X.X.zip`.](https://github.com/google/dwh-migration-tools/releases/latest)**

The currently available tools are:

- **Metadata and Log Dumper:** Utility for connecting to an existing database
and generating an archive of DDL metadata or logs for consumption by Assessment
or the Translation Service. For more information, [read the tool
documentation](https://cloud.google.com/bigquery/docs/generate-metadata).

    To run the Dumper `Java 8` or  higher is required. Third party JDBC drivers might
  impose additional restrictions on Java versions. Refer to the JDBC driver's manual for details.

- **Batch SQL Translation Client:** Command line utility to run a Batch SQL
Translation job with support for macro expansion/unexpansion. For more
information, [read how to submit a translation job using the
client](https://cloud.google.com/bigquery/docs/batch-sql-translator#submit_a_translation_job)
and view the [installation instructions](client/README.md).

- **Dbsync Tools (work in progress):** Command line utility to sync large
  files into Google Cloud Storage. View the [documentation](dbsync/README.md).

## Databricks Dumper

The `databricks` connector dumps tables registered in Unity Catalog. It can
also dump tables in the legacy `hive_metastore` catalog when a Databricks SQL
warehouse ID is provided with `--warehouse`.

### Unity Catalog only

Without `--warehouse`, the connector uses the Unity Catalog REST API and dumps
Unity Catalog tables and their table metadata, including column definitions:

```bash
./gradlew :dumper:app:run --args="--connector databricks \
  --url https://<workspace-host> \
  --password <databricks-token>"
```

### Unity Catalog and Hive Metastore

With `--warehouse <sql-warehouse-id>`, the connector dumps both Unity Catalog
and Hive Metastore tables. Hive Metastore metadata is collected through the
Databricks SQL Statement Execution API using the configured SQL warehouse.
For each Hive Metastore table, the connector collects column definitions with
`DESCRIBE TABLE` and writes them in the same `meta.columns` format used for
Unity Catalog tables.

```bash
./gradlew :dumper:app:run --args="--connector databricks \
  --url https://<workspace-host> \
  --password <databricks-token> \
  --warehouse <sql-warehouse-id>"
```

The token must be authorized to access the workspace and execute statements on
the SQL warehouse. The output JSONL identifies legacy tables with
`catalog: "hive_metastore"`; for example,
`hive_metastore.default.example_table`.

## Compiling from source
You need to have `JDK 8` installed. For multiple jdk versions we recommend to use https://sdkman.io/ 
### Build all the modules ###
    
    ./gradlew build
### Build the DWH Dumper ###
    
    ./gradlew :dumper:app:build

## Contributing

Contributing instructions are available, per tool, at the following locations:
- [Metadata and Log Dumper contribution guide](dumper/CONTRIBUTING.md)
- [Batch SQL Translation Client contribution guide](client/CONTRIBUTING.md)

## License

Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

