# Client Side Validation Utility

The BigQuery Migration Service Validation utility ensures data accuracy and completeness throughout the
migration process via SQL. It requires access to the source DB on-prem and to Google Cloud Storage (GCS) and BigQuery.

The validation utility has the following goals:
1. Query source and target metadata tables for schema compatibility check.
2. Query source DB for aggregate and row sample and export results to GCS.
3. Query target DB for equivalent aggregate and row sample.
4. Compare source and target results in Google BigQuery via SQL.
5. Provide summary of comparison in a BQ table.

## Connections
Connection files should be placed in the default validation directory ~/.config/dwh-validation/ or a 
directory specified by the env variable `DV_CONN_HOME`.

Sample PostgreSQL connection file:
```json
{
  "connectionType": "postgresql",
  "database": "database",
  "driver": "path/to/postgresql-version.jar",
  "user": "user",
  "password": "password",
  "host": "localhost",
  "port": 5432
}
```

## License

Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.