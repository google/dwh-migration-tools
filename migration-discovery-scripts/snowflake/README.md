# ❄️ Snowflake Discovery Scripts

Lightweight, native Snowflake discovery scripts to collect workload, credit consumption, and storage metrics for **BigQuery Migration Assessment (TCO analysis)**.

Unlike `dwh-migration-dumper`, these scripts require **zero Java runtime**, extract **zero SQL query text (`QUERY_TEXT`), table/schema names, or PII**, and package the output into `snowflake_assessment_tco.zip`.

## 🚀 Prerequisites

- **Database Permissions**: Read access to `SNOWFLAKE.ACCOUNT_USAGE` (e.g. `ACCOUNTADMIN`, `SYSADMIN`, or a role with `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`).
  - *Note*: If `ACCOUNT_USAGE.ACCESS_HISTORY` is unavailable (e.g. Snowflake Standard Edition), the script automatically falls back to `IsMultiObject = FALSE`.
- **For CLI Execution (Option 1)**: [SnowSQL CLI](https://docs.snowflake.com/en/user-guide/snowsql-install-config) and standard shell utilities (`bash`, `sed`, `awk`, `zip`).

---

## 💻 Option 1: Automated CLI (`tco_discovery.sh`)

### 1. Authenticate
Configure one of the following before running:
- **Password**: `export SNOWSQL_PWD='<password>'`
- **Key-pair**: Pass `-k /path/to/rsa_key.p8` (and `export SNOWSQL_PRIVATE_KEY_PASSPHRASE='<passphrase>'` if encrypted)
- **Config file**: Set credentials under `[connections]` in `~/.snowsql/config`

### 2. Run Extraction

```bash
./tco_discovery.sh -a <account> -u <user> [-r <role>] [-w <warehouse>] [-d <days>]
```

| Flag | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `-a, --account` | **Yes** | — | Snowflake account identifier (e.g. `xy12345.us-east-1` or `org-account`). |
| `-u, --user` | **Yes** | — | Snowflake username. |
| `-r, --role` | No | User default | Role with `ACCOUNT_USAGE` access (e.g. `ACCOUNTADMIN`). |
| `-w, --warehouse` | No | Active WH | Warehouse to run queries (X-Small recommended for 14d; Small/Medium for >30d). |
| `-k, --private-key-path` | No | — | Path to RSA private key file (`rsa_key.p8`). |
| `-d, --days` | No | `14` | Historical lookback window in days (`1`–`365`). |
| `-o, --output` | No | `./out` | Output directory for extracted CSVs and metadata. |

**Examples:**
```bash
# Standard 14-day discovery
export SNOWSQL_PWD='<your_snowflake_password>'
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH

# 365-day annual lookback with RSA key-pair auth
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -k rsa_key.p8 -d 365
```

---

## 🌐 Option 2: Manual Execution via Snowsight (Web UI)

If CLI access is restricted, run the queries directly in **Snowsight → Worksheets**:

1. **Set ISO-8601 timestamp format**:
   ```sql
   ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"';
   ```
2. **Run each SQL script in `sql/` and download results as CSV**:
   - `sql/warehouses_workload_structure_mds.sql` → `warehouses_workload_structure_mds.csv` *(adjust `SET LOOKBACK_DAYS = 14;` at line 1 if needed; on Standard Edition without `ACCESS_HISTORY`, replace `MultiObjectQueries` body with `SELECT CAST(NULL AS VARCHAR) AS QUERY_ID WHERE 1=0`)*
   - `sql/warehouse_metering_history_mds.sql` → `warehouse_metering_history_mds.csv`
   - `sql/table_storage_metrics_mds.sql` → `table_storage_metrics_mds.csv`
3. **Create `compilerworks-metadata.yaml`** in the same directory:
   ```yaml
   format: "snowflake_assessment_tco.zip"
   timestamp: 1760000000000
   product:
     version: "migration-discovery-scripts:1.0.0"
     arguments: "ConnectorArguments{connector=snowflake-mds, assessment=true}"
   ```
4. **Package into `snowflake_assessment_tco.zip`**:
   ```bash
   zip snowflake_assessment_tco.zip \
     warehouses_workload_structure_mds.csv \
     warehouse_metering_history_mds.csv \
     table_storage_metrics_mds.csv \
     compilerworks-metadata.yaml
   ```

---

## 📤 Next Steps

1. Upload `snowflake_assessment_tco.zip` to Google Cloud Storage:
   ```bash
   gcloud storage cp snowflake_assessment_tco.zip gs://<your-assessment-bucket>/
   ```
2. In the **Google Cloud Console**, open **BigQuery → Migration Assessment**, create an assessment with **Snowflake** as the data source, and select your GCS bucket.
