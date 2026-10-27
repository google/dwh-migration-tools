# ❄️ Snowflake Discovery Scripts for BigQuery TCO Assessment

Lightweight, native Snowflake discovery scripts to collect workload structure, hourly warehouse metering, and table storage metrics for **BigQuery Migration Assessment (TCO modeling)**.

Zero Java runtime, zero SQL text, table/schema names, or PII extracted. Automatically falls back to `IsMultiObject = FALSE` on Snowflake Standard Edition (inaccessible `ACCESS_HISTORY`). SQL templates in `sql/` are the single source of truth across all options, producing `snowflake_assessment_tco.zip`.

## Choose an Option

| Option | How you run it | Privileges | Output |
| :--- | :--- | :--- | :--- |
| **1. Python Worksheet**| Snowsight UI | `IMPORTED PRIVILEGES` on `SNOWFLAKE` db, `USAGE` on warehouse, db, schema; `CREATE STAGE` on schema (or existing `BQMS_TCO_DISCOVERY_STAGE`) | Pre-signed URL or stage download |
| **2. Notebook** | Snowsight UI | Same as Option 1 + `CREATE NOTEBOOK` on schema | Pre-signed URL or stage download |
| **3. CLI Script** | Local terminal | `IMPORTED PRIVILEGES` on `SNOWFLAKE` db, `USAGE` on warehouse; local SnowSQL CLI, bash, zip | Local zip file |
| **4. Manual SQL** | Snowsight UI | `IMPORTED PRIVILEGES` on `SNOWFLAKE` db, `USAGE` on warehouse | 4 downloaded CSVs + manual zip |

---

### Option 1: Snowsight Python Worksheet

Supported in both the **Legacy Snowsight UI** and **New Snowsight Workspaces UI**:

- **Legacy Snowsight UI**:
  1. Open **Projects → Worksheets**, click **+** → **Python Worksheet**.
  2. Select an active **Database**, **Schema**, and **Warehouse** in the context selector.
  3. In worksheet **Settings** (gear icon), ensure **Handler** = `main` and **Return type** = `Table()`.
  4. Paste [`snowflake_tco_discovery_worksheet.py`](./snowflake_tco_discovery_worksheet.py). By default, it extracts the last 14 days (`START_DATE` and `END_DATE` default to `None`, covering today - 14 days to today UTC). To extract a specific date range, set `START_DATE = 'YYYY-MM-DD'` and `END_DATE = 'YYYY-MM-DD'` near the top (the end date is inclusive: the window covers through 23:59:59 UTC of that date).
  5. Click **Run**. The result table displays `STEP`, `STATUS`, and `DETAIL`.

- **New Snowsight Workspaces UI**:
  1. Click the big **+** → **Python file** and paste [`snowflake_tco_discovery_worksheet.py`](./snowflake_tco_discovery_worksheet.py) (it runs top-to-bottom as a script).
  2. If no database/schema is selected, set `OUTPUT_DATABASE` and `OUTPUT_SCHEMA` at the top of the file. Adjust `START_DATE` and `END_DATE` as needed.
  3. Click **Run** and confirm **Connect your Python file** (Container Runtime compute pool). Requires compute-pool quota; if you see a *node limit* error, use the Legacy UI, **Option 3 (CLI)** or **Option 4 (Manual SQL)**.

Once complete, open the `DOWNLOAD_URL` to download `snowflake_assessment_tco.zip`, or download from **Data → Databases → <db> → <schema> → Stages → BQMS_TCO_DISCOVERY_STAGE**.

### Option 2: Snowflake Notebook

Supported in both legacy warehouse environments and Snowsight Workspaces:

- **Legacy Snowsight UI**:
  1. Open **Projects → Notebooks**, click the arrow next to **+ Notebook** → **Import .ipynb file**.
  2. Select [`snowflake_tco_discovery.ipynb`](./snowflake_tco_discovery.ipynb), choose your **Database**, **Schema**, and **Warehouse**, then import.
  3. By default, it extracts the last 14 days (`START_DATE` and `END_DATE` default to `None`, covering today - 14 days to today UTC). To extract a specific date range, set `START_DATE = 'YYYY-MM-DD'` and `END_DATE = 'YYYY-MM-DD'` in the first configuration cell (the end date is inclusive: the window covers through 23:59:59 UTC of that date).
  4. Click **Run All**.

- **New Snowsight Workspaces UI**:
  1. Click the big **+** → **Notebook** and import [`snowflake_tco_discovery.ipynb`](./snowflake_tco_discovery.ipynb).
  2. If no database/schema is selected, set `OUTPUT_DATABASE` and `OUTPUT_SCHEMA` in the configuration cell. Adjust `START_DATE` and `END_DATE` as needed.
  3. Click **Run all** and confirm **Connect your notebook** (Container Runtime compute pool). Requires compute-pool quota; if you see a *node limit* error, use the Legacy UI, **Option 3 (CLI)** or **Option 4 (Manual SQL)**.

Use the printed pre-signed URL or stage location to download `snowflake_assessment_tco.zip`.

---

### Option 3: Automated CLI (`tco_discovery.sh`)
```bash
export SNOWSQL_PWD='<password>'  # or pass -k /path/to/rsa_key.p8
./tco_discovery.sh -a <account> -u <user> [-r <role>] [-w <warehouse>] [-s <start_date>] [-e <end_date>] [-o <output_dir>]
```

| Flag | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `-a, --account` | **Yes** | — | Snowflake account identifier (e.g. `xy12345.us-east-1` or `org-account`). |
| `-u, --user` | **Yes** | — | Snowflake username. |
| `-r, --role` | No | User default | Role with `ACCOUNT_USAGE` access (e.g. `ACCOUNTADMIN`). |
| `-w, --warehouse` | No | Active WH | Warehouse to run queries (X-Small for 14d; Small/Medium for >30d). |
| `-k, --private-key-path` | No | — | Path to RSA private key file (`rsa_key.p8`) for key-pair auth. |
| `-s, --start-date` | No | `today - 14 days` | Extraction start date (`YYYY-MM-DD`, UTC 00:00:00). |
| `-e, --end-date` | No | `today (UTC)` | Extraction end date (`YYYY-MM-DD`, inclusive through 23:59:59 UTC). |
| `-o, --output` | No | `./out` | Output staging directory for extracted CSVs and zip archive. |

> **Note on Date Ranges & Retention:**
> - Snowflake `ACCOUNT_USAGE` retains historical metadata for up to **365 days**. Start dates older than 365 days or dates in the future are rejected.
> - The end date is inclusive: passing `-s 2026-02-01 -e 2026-05-10` sets a query window of `[2026-02-01 00:00:00 UTC, 2026-05-11 00:00:00 UTC)`.

**Examples:**
```bash
# Default extraction (last 14 days)
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH

# Custom date range (Feb 1, 2026 through May 10, 2026 inclusive)
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -s 2026-02-01 -e 2026-05-10

# Custom date range with key-pair auth
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -k rsa_key.p8 -s 2026-02-01 -e 2026-05-10
```

---

### Option 4: Manual Execution in Snowsight
1. Set session parameters:
   ```sql
   ALTER SESSION SET TIMEZONE='UTC', TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"';
   ```
2. Run each script in `sql/` and download results as CSV. Configure the two `SET` statements at the top of each time-filtered script (`warehouses_workload_structure_mds.sql`, `warehouse_metering_history_mds.sql`, `report_date_range_mds.sql`) to match:
   - **For default (last 14 days):**
     ```sql
     SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
     SET END_DATE = CURRENT_DATE();
     ```
   - **For a custom date range (e.g., Feb 1, 2026 to May 10, 2026 inclusive):**
     ```sql
     SET START_DATE = '2026-02-01'::DATE;
     SET END_DATE = '2026-05-10'::DATE;
     ```
     *(Note: $END_DATE is inclusive; queries automatically evaluate `< DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ`. Max retention is 365 days due to Snowflake `ACCOUNT_USAGE` retention).*

   Execute and download:
   - `sql/warehouses_workload_structure_mds.sql` → `warehouses_workload_structure_mds.csv` *(on Standard Edition, replace `MultiObjectQueries` CTE with `SELECT CAST(NULL AS VARCHAR) AS QUERY_ID WHERE 1=0`)*
   - `sql/warehouse_metering_history_mds.sql` → `warehouse_metering_history_mds.csv`
   - `sql/report_date_range_mds.sql` → `report_date_range.csv`
   - `sql/table_storage_metrics_mds.sql` → `table_storage_metrics_mds.csv` *(captures a point-in-time snapshot of current table storage; historical date filtering does not apply)*
3. Generate `compilerworks-metadata.yaml`:
   ```bash
   cat <<EOF > compilerworks-metadata.yaml
   format: "snowflake_assessment_tco.zip"
   timestamp: $(($(date +%s) * 1000))
   product:
     version: "migration-discovery-scripts:1.0.0"
     arguments: "ConnectorArguments{connector=snowflake-mds, assessment=true}"
   EOF
   ```
4. Generate `dumper-telemetry.jsonl` (run `SELECT CURRENT_REGION();` in Snowsight to find your account region; set `ACCESS_HISTORY_AVAILABLE=false` if you applied the Standard Edition fallback; set `START_DATE` and `END_DATE`):
   ```bash
   START_DATE="2026-02-01"      # e.g. "2026-02-01" or start date used in queries
   END_DATE="2026-05-10"        # e.g. "2026-05-10" or end date used in queries
   # Note: For END_DATE_PLUS_1, add 1 day (e.g. 2026-05-11 for 2026-05-10)
   END_DATE_PLUS_1="2026-05-11"
   ACCOUNT_REGION='<result of SELECT CURRENT_REGION();>'
   ACCESS_HISTORY_AVAILABLE=true
   NOW_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
   RUN_DATE=$(date -u +%Y-%m-%d)
   RUN_ID="snowflake-discovery-$(date -u +%Y%m%d%H%M%S)"
   WORKLOAD_ROWS=$(($(wc -l < warehouses_workload_structure_mds.csv) - 1))
   METERING_ROWS=$(($(wc -l < warehouse_metering_history_mds.csv) - 1))
   STORAGE_ROWS=$(($(wc -l < table_storage_metrics_mds.csv) - 1))
   DATE_RANGE_ROWS=$(($(wc -l < report_date_range.csv) - 1))

   cat <<EOF > dumper-telemetry.jsonl
   {"id":"$RUN_ID","eventType":"DUMPER_RUN_METRICS","dumperRunStartClientTime":"$NOW_ISO","dumperMetadata":{"version":"1.0.0","gitCommit":"migration-discovery-scripts","buildDate":"$RUN_DATE"},"payload":[{"id":"snowflake-tco-discovery","measureStartTime":"$NOW_ISO","measureEndTime":"$NOW_ISO","started_at":"$NOW_ISO","completed_at":"$NOW_ISO","runDurationInMinutes":0,"duration_seconds":0,"overallStatus":"SUCCESS","status":"SUCCESS","failed_tasks":0,"error_summary":"","taskExecutionSummary":[{"count":4,"status":"SUCCEEDED"},{"count":0,"status":"FAILED"}],"taskDetailedSummary":[{"name":"warehouses_workload_structure_mds.csv","type":"REQUIRED","status":"SUCCEEDED","duration_seconds":0,"row_count":$WORKLOAD_ROWS},{"name":"warehouse_metering_history_mds.csv","type":"REQUIRED","status":"SUCCEEDED","duration_seconds":0,"row_count":$METERING_ROWS},{"name":"table_storage_metrics_mds.csv","type":"REQUIRED","status":"SUCCEEDED","duration_seconds":0,"row_count":$STORAGE_ROWS},{"name":"report_date_range.csv","type":"REQUIRED","status":"SUCCEEDED","duration_seconds":0,"row_count":$DATE_RANGE_ROWS},{"name":"compilerworks-metadata.yaml","type":"REQUIRED","status":"SUCCEEDED","duration_seconds":0}],"argumentSummary":{"connector":"snowflake-mds","assessment":true,"account_region":"$ACCOUNT_REGION","access_history_available":$ACCESS_HISTORY_AVAILABLE,"extraction_method":"snowflake-manual","date_range_start":"${START_DATE}T00:00:00Z","date_range_end":"${END_DATE_PLUS_1}T00:00:00Z"}}]}
   EOF
   ```
5. Package into `snowflake_assessment_tco.zip`:
   ```bash
   zip snowflake_assessment_tco.zip warehouses_workload_structure_mds.csv warehouse_metering_history_mds.csv table_storage_metrics_mds.csv report_date_range.csv compilerworks-metadata.yaml dumper-telemetry.jsonl
   ```

---

## 🛠️ For Maintainers

SQL scripts in `sql/*.sql` are the single source of truth. When modifying SQL queries, regenerate the native artifacts and verify synchronization:
```bash
python3 build_notebook.py
python3 build_notebook.py --check
```

---

## 📤 Next Steps

1. Upload `snowflake_assessment_tco.zip` to Google Cloud Storage:
   ```bash
   gcloud storage cp snowflake_assessment_tco.zip gs://<your-assessment-bucket>/
   ```
2. In the **Google Cloud Console**, navigate to **BigQuery → Migration Assessment**, click **Create Assessment**, select **Snowflake** as the data source, and point to your GCS bucket path.
