# 💻 Oracle Database Discovery Scripts

This package provides easy to use Oracle discovery scripts and automates the collection of key performance and configuration metrics, which are essential for a comprehensive Total Cost of Ownership (TCO) analysis.  

The script leverages `SQL*Plus` to execute a series of predefined SQL queries, capturing valuable information about historical performance, storage utilization, and system configuration. Unlike `dwh-migration-dumper`, which is a Java application, these scripts use only native Oracle tooling so they can be used by the DBAs with no additional effort.

Retrieved Oracle data is saved in CSV format accepted by BigQuery Migrations Assessment.

## 🚀 Getting Started

### Prerequisites

To use this script, you must have the following installed and configured on your system:

- **Oracle Client**: A full Oracle Client or the Oracle Instant Client.
- **SQL*Plus**: The sqlplus command-line utility must be accessible from your system's PATH.
- **Database Permissions**: An Oracle common user with SYSDBA privileges.

Please note that script must be executed in the root container. Running it in one of the pluggable databases results in missing performance statistics and metadata about other pluggable databases.

### Usage

Run the script from your terminal, passing the Oracle connection string as the first argument.

```bash
./tco_discovery.sh <ORACLE_CONN_STR>
```

**Example:**

```bash
./tco_discovery.sh system/manager@192.168.1.10:1521
```

## 🛠️ Configuration

The script's behavior can be customized by modifying the variables within the script itself:

- `ORACLE_CONN`: The Oracle connection string. This is passed as a command-line argument, as shown in the usage example.
- `DURATION_DAYS`: The number of days of historical data to collect for AWR-based queries. Defaults to 30.
- `OUTPUT_DIR`: The directory where the generated CSV files will be stored. The script will create this directory if it doesn't already exist. Defaults to ./out.
- `DISCOVERY_SQLS`: An array of SQL script filenames to be executed. You can easily add or remove scripts from this list to customize your data collection.

## 📂 Output

Upon successful execution, the script creates the specified OUTPUT_DIR (e.g., ./out) and populates it with a series of CSV files. Each file is named after the corresponding SQL script and contains the data captured from the database.
