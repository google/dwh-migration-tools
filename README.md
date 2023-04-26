# Data Warehouse Migration Tools
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This project contains a collection of tools related to the [BigQuery Migration
Service](https://cloud.google.com/bigquery/docs/migration-intro).

**[Download the latest cross-platform release zip `dwh-migration-tools-vX.X.X.zip`.](https://github.com/google/dwh-migration-tools/releases/latest)**

The currently available tools are:

- **Metadata and Log Dumper:** Utility for connecting to an existing database
and generating an archive of DDL metadata or logs for consumption by Assessment
or the Translation Service. For more information, [read the tool
documentation](https://cloud.google.com/bigquery/docs/generate-metadata).

- **Batch SQL Translation Client:** Command line utility to run a Batch SQL
Translation job with support for macro expansion/unexpansion. For more
information, [read how to submit a translation job using the
client](https://cloud.google.com/bigquery/docs/batch-sql-translator#submit_a_translation_job)
and view the [installation instructions](client/README.md).

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

