# Data Warehouse Discovery Scripts
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This directory contains lightweight, native scripts that can be run on top of data warehouses to collect the metadata and operational metrics needed for BigQuery Migration Assessment (TCO modeling).

## Available Discovery Packages

- **[Oracle](./oracle/)**: Native SQL*Plus extraction scripts for Oracle Database / Exadata to extract instance sizing, storage metrics, and performance statistics without full Java dumper execution.
- **[Snowflake](./snowflake/)**: Native SnowSQL / Snowsight extraction scripts for Snowflake to extract virtual warehouse workload distributions, compute credit consumption, and aggregated storage metrics with zero external binaries.