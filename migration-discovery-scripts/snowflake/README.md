# ❄️ Snowflake Discovery & TCO Extraction Scripts

This package provides native Snowflake discovery scripts to automate the collection of key performance, utilization, and configuration metrics essential for BigQuery Total Cost of Ownership (TCO) analysis.

Unlike the traditional Java-based `dwh-migration-dumper`, these scripts use **native Snowflake tooling only** (`snowsql` or Snowsight Web UI worksheets). They require zero JVM runtime installation, extract **zero customer SQL text or PII**, and can be executed by database administrators or cloud architects in minutes.

The extracted metadata is packaged into a standard ZIP archive (`snowflake_assessment_tco.zip`) accepted directly by the **BigQuery Migration Assessment Service**.

---

## 🚀 Prerequisites & Permissions

1. **SnowSQL (Option 1)**: Snowflake Command Line Interface ([SnowSQL Installation Guide](https://docs.snowflake.com/en/user-guide/snowsql-install-config)).
2. **Database Permissions**: A Snowflake role with read access to the `SNOWFLAKE.ACCOUNT_USAGE` shared schema (e.g. `ACCOUNTADMIN`, `SYSADMIN`, or a custom audit role granted `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`).
   - *Note on `ACCESS_HISTORY`*: The scripts automatically inspect multi-object queries using `ACCOUNT_USAGE.ACCESS_HISTORY`. If your account is on Snowflake Standard Edition or lacks `GOVERNANCE_VIEWER` access, the extraction script gracefully falls back to `IsMultiObject = FALSE` without failing.
3. **Local Utilities (Option 1)**: `bash`, `sed`, `awk`, and `zip`.

---

## 💻 Extraction Workflow

### Option 1: Automated CLI Execution via `tco_discovery.sh` (Recommended)

1. **Configure Authentication** (choose one before running the script, as `snowsql` is invoked multiple times non-interactively):
   - **Password Authentication**: Export `SNOWSQL_PWD` in your shell session (prefix with a leading space if `HISTCONTROL=ignorespace` is enabled to keep it out of shell history):
     ```bash
     export SNOWSQL_PWD='<your_snowflake_password>'
     ```
   - **Key-Pair Authentication**: Pass `-k /path/to/rsa_key.p8` (and if your private key is encrypted, `export SNOWSQL_PRIVATE_KEY_PASSPHRASE='<passphrase>'`).
   - **SnowSQL Config File**: Configure `password` or `private_key_path` under `[connections]` in `~/.snowsql/config`.

2. **Run `tco_discovery.sh`** passing your Snowflake account and username:

```bash
./tco_discovery.sh \
  -a <account_identifier> \
  -u <username> \
  -r <role> \
  -w <warehouse>
```

#### Command Options

| Flag | Argument | Required? | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `-a, --account` | `ACCOUNT` | **Yes** | — | Snowflake account identifier (e.g. `xy12345.us-east-1` or `org-account`). |
| `-u, --user` | `USERNAME` | **Yes** | — | Snowflake database username. |
| `-r, --role` | `ROLE` | No | User default | Snowflake role with `ACCOUNT_USAGE` privileges (e.g. `ACCOUNTADMIN`). |
| `-w, --warehouse` | `WAREHOUSE` | No | Active WH | Virtual warehouse for query execution (e.g. `COMPUTE_WH`, typically X-Small). |
| `-k, --private-key-path` | `FILE` | No | — | Path to RSA private key file (e.g. `rsa_key.p8`) for key-pair authentication. |
| `-d, --days` | `DAYS` | No | `14` | Lookback period in days (1 to 365). |
| `-o, --output` | `DIR` | No | `./out` | Local staging directory for output files. |
| `-h, --help` | — | No | — | Display usage syntax. |

#### Examples

```bash
# Standard 14-day discovery with password authentication
export SNOWSQL_PWD='<your_snowflake_password>'
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH

# Using RSA key-pair authentication
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -k /path/to/rsa_key.p8

# 365-day annual lookback for seasonal TCO modeling
./tco_discovery.sh -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -d 365
```

> [!TIP]
> **Warehouse Sizing Guidance for Long Lookbacks**:
> - For default 14-day lookbacks (`-d 14`), an **X-Small** warehouse executes in ~1–3 minutes consuming `< 0.05` credits.
> - For longer lookbacks (> 30 days) on large enterprise accounts with tens of millions of queries, specify a **Small** or **Medium** warehouse (`-w`) and increase statement timeout if necessary. All joined views include timestamp pruning filters.

The script runs the pre-flight checks, executes the queries sequentially, cleans multi-statement preamble headers, generates `compilerworks-metadata.yaml` (connector `snowflake-mds`) alongside telemetry (`dumper-telemetry.jsonl`), and packages all artifacts into `snowflake_assessment_tco.zip`.

---

### Option 2: Manual Execution via Snowflake Snowsight (Web Console)

If running command-line utilities is restricted by corporate security policies, DBAs can execute the SQL queries directly inside Snowflake Snowsight:

1. Open **Snowsight** $\rightarrow$ **Worksheets**.
2. Set session timestamp output formats (ensuring ISO-8601 formatting required by BigQuery Migration Assessment):
   ```sql
   ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"', TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"';
   ```
3. Run each SQL script in `sql/` and download the results as CSV:
   - **Historical time-filtered scripts** (each begins with `SET LOOKBACK_DAYS = 14;` allowing you to adjust the lookback window directly in the session variable):
     - `sql/warehouses_workload_structure_mds.sql` $\rightarrow$ Download CSV as `warehouses_workload_structure_mds.csv`
     - `sql/warehouse_metering_history_mds.sql` $\rightarrow$ Download CSV as `warehouse_metering_history_mds.csv`
   - **Current state snapshot script** (extracts current account storage state; no lookback variable required):
     - `sql/table_storage_metrics_mds.sql` $\rightarrow$ Download CSV as `table_storage_metrics_mds.csv`
4. Create a metadata descriptor file named `compilerworks-metadata.yaml` in the same directory as the downloaded CSVs:
   - **From a terminal (Linux / macOS / Git Bash / WSL)** — automatically injects the current Unix epoch timestamp in milliseconds:
     ```bash
     cat <<EOF > compilerworks-metadata.yaml
     format: "snowflake_assessment_tco.zip"
     timestamp: $(($(date +%s) * 1000))
     product:
       version: "migration-discovery-scripts:1.0.0"
       arguments: "ConnectorArguments{connector=snowflake-mds, assessment=true}"
     EOF
     ```
   - **Or manually in a text editor** — replace `<CURRENT_EPOCH_MS>` with the output of `SELECT DATE_PART(EPOCH_MILLISECOND, CURRENT_TIMESTAMP());` in Snowsight (or omit the `timestamp:` line, which is optional):
     ```yaml
     format: "snowflake_assessment_tco.zip"
     timestamp: <CURRENT_EPOCH_MS>
     product:
       version: "migration-discovery-scripts:1.0.0"
       arguments: "ConnectorArguments{connector=snowflake-mds, assessment=true}"
     ```
5. Package the 3 CSVs and `compilerworks-metadata.yaml` into a ZIP archive:
   ```bash
   zip snowflake_assessment_tco.zip *.csv compilerworks-metadata.yaml
   ```

---

## 📤 Next Steps: Running BigQuery Migration Assessment

1. Upload the generated `snowflake_assessment_tco.zip` to your Google Cloud Storage bucket:
   ```bash
   gcloud storage cp snowflake_assessment_tco.zip gs://<your-assessment-bucket>/
   ```
2. Navigate to **BigQuery Migration Assessment** in the Google Cloud Console.
3. Click **Create Assessment**, select **Snowflake** as the source, point the input path to your GCS bucket, and run the assessment.
4. Review the generated Looker Studio Bridge Report for slot sizing recommendations and cost comparisons.
