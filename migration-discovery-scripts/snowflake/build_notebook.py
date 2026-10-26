#!/usr/bin/env python3
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Generates native Snowflake discovery artifacts from SQL templates.

Produces:
1. snowflake_tco_discovery.ipynb (Snowflake Notebook)
2. snowflake_tco_discovery_worksheet.py (Snowsight Python Worksheet)
"""

import argparse
import json
import os
import pathlib
import sys

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
SQL_DIR = SCRIPT_DIR / "sql"
DEFAULT_OUTPUT_NOTEBOOK = SCRIPT_DIR / "snowflake_tco_discovery.ipynb"
DEFAULT_OUTPUT_WORKSHEET = SCRIPT_DIR / "snowflake_tco_discovery_worksheet.py"

# License headers (Apache 2.0)
LICENSE_HEADER_HASH = """# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""

# =====================================================================
# Shared Python Logic
# =====================================================================

SHARED_RENDER_SQL_CODE = '''def render_sql(raw_sql: str, lookback_days: int, access_history_available: bool = True) -> str:
    """Renders SQL template by removing SET LOOKBACK_DAYS and substituting variables."""
    # 1. Strip SET LOOKBACK_DAYS = N; line case-insensitively
    lines = []
    for line in raw_sql.splitlines():
        if re.match(r"^\\s*SET\\s+LOOKBACK_DAYS\\s*=\\s*\\d+;\\s*$", line, re.IGNORECASE):
            continue
        lines.append(line)
    sql = "\\n".join(lines)

    # 2. Substitute variable placeholders
    sql = sql.replace("$LOOKBACK_DAYS", str(lookback_days))
    sql = sql.replace("&1", str(lookback_days))

    # 3. If ACCESS_HISTORY is unavailable, replace MultiObjectQueries CTE body
    if not access_history_available and "MultiObjectQueries AS (" in sql:
        new_lines = []
        skip = False
        for line in sql.splitlines():
            if "MultiObjectQueries AS (" in line:
                new_lines.append("MultiObjectQueries AS (")
                new_lines.append("    SELECT CAST(NULL AS VARCHAR) AS QUERY_ID WHERE 1=0")
                new_lines.append("),")
                skip = True
                continue
            if skip:
                if re.match(r"^\\s*\\),", line):
                    skip = False
                continue
            new_lines.append(line)
        sql = "\\n".join(new_lines)

    return sql'''

SHARED_FORMAT_VALUE_CODE = '''def format_value(val) -> str:
    """Formats values for CSV export matching BQMS Assessment ingestion contracts."""
    if val is None:
        return ""
    if isinstance(val, datetime.datetime):
        if val.tzinfo is not None:
            val = val.astimezone(datetime.timezone.utc)
        return val.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(val, datetime.date):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return format(val, "f")
    if isinstance(val, float):
        # Matches SnowSQL FLOAT rendering exactly (10 significant digits,
        # e.g. '1', '0.999999', '1.055555556e-05').
        return format(val, ".10g")
    return str(val)'''

SHARED_QUOTE_IDENT_CODE = '''def quote_ident(ident: str) -> str:
    """Quotes a Snowflake identifier if it contains lowercase or special characters."""
    if not ident or not isinstance(ident, str):
        raise ValueError(f"Invalid identifier: {ident!r}")
    if re.match(r"^[A-Za-z_][A-Za-z0-9_$]*$", ident):
        return ident
    return '"' + ident.replace('"', '""') + '"' '''



def load_sql_templates() -> dict[str, str]:
    """Reads SQL files which remain the single source of truth."""
    files = {
        "warehouses_workload_structure_mds": SQL_DIR / "warehouses_workload_structure_mds.sql",
        "warehouse_metering_history_mds": SQL_DIR / "warehouse_metering_history_mds.sql",
        "table_storage_metrics_mds": SQL_DIR / "table_storage_metrics_mds.sql",
    }
    templates = {}
    for key, path in files.items():
        if not path.is_file():
            raise FileNotFoundError(f"Missing required SQL template: {path}")
        templates[key] = path.read_text(encoding="utf-8")
    return templates


def format_sql_constants(templates: dict[str, str]) -> str:
    """Formats SQL templates into embedded Python string constants."""
    escaped_workload = templates["warehouses_workload_structure_mds"].replace('"""', r'\"\"\"')
    escaped_metering = templates["warehouse_metering_history_mds"].replace('"""', r'\"\"\"')
    escaped_storage = templates["table_storage_metrics_mds"].replace('"""', r'\"\"\"')
    return f'''# Embedded SQL templates (single source of truth: sql/*.sql)
SQL_WAREHOUSES_WORKLOAD_STRUCTURE = """{escaped_workload}"""

SQL_WAREHOUSE_METERING_HISTORY = """{escaped_metering}"""

SQL_TABLE_STORAGE_METRICS = """{escaped_storage}"""

EXTRACTION_TASKS = [
    (
        "warehouses_workload_structure_mds.sql",
        "warehouses_workload_structure_mds.csv",
        SQL_WAREHOUSES_WORKLOAD_STRUCTURE,
        "Virtual Warehouse Workload Structure",
    ),
    (
        "warehouse_metering_history_mds.sql",
        "warehouse_metering_history_mds.csv",
        SQL_WAREHOUSE_METERING_HISTORY,
        "Virtual Warehouse Metering History",
    ),
    (
        "table_storage_metrics_mds.sql",
        "table_storage_metrics_mds.csv",
        SQL_TABLE_STORAGE_METRICS,
        "Table Storage Breakdown",
    ),
]'''


# =====================================================================
# Notebook Generator
# =====================================================================

def build_notebook_dict(templates: dict[str, str]) -> dict:
    """Constructs the nbformat 4 notebook dictionary."""

    cell_1_markdown = """# ❄️ Snowflake Discovery & TCO Extraction for BigQuery Migration

Native Snowflake discovery notebook to collect workload structure, hourly warehouse metering, and table storage metrics for **BigQuery Migration Assessment (TCO modeling)**.

### What it does
- Collects virtual warehouse workload structure, hourly credit consumption, and table storage metrics directly from `SNOWFLAKE.ACCOUNT_USAGE`.
- **Zero Query Text / PII**: Extracts **zero** SQL query text (`QUERY_TEXT`), customer table/schema names, or PII.
- Automatically handles edition differences (falls back gracefully if `ACCESS_HISTORY` is inaccessible, e.g. on Snowflake Standard Edition).
- Packages all outputs into a single zip archive (`snowflake_assessment_tco.zip`) on a Snowflake internal stage and provides a pre-signed download URL.

### Required Privileges
- **Database & Schema**: `USAGE` on the database and schema where this notebook runs.
- **Account Usage**: Read access to `SNOWFLAKE.ACCOUNT_USAGE` (e.g. `ACCOUNTADMIN`, `SYSADMIN`, or a role granted `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`).
- **Warehouse**: `USAGE` privilege on a virtual warehouse (X-Small is sufficient for standard 14-day discovery; Small or Medium recommended for lookbacks >30 days).
- **Stage**: Privilege to create an internal stage (`CREATE STAGE` on the current schema) or access to the output stage.

### Snowflake Environment & Runtime Note
- **Legacy Snowsight UI**: Notebook runs on a standard virtual warehouse. Select your active Database, Schema, and Warehouse in the notebook context selector.
- **New Snowsight Workspaces UI**: Notebooks may run on Snowflake Container Runtime, which requires compute-pool quota (`CREATE COMPUTE POOL` or compute pool access). If your account lacks Container Runtime compute-pool quota, run the notebook on a standard warehouse in the legacy UI, or use **Option 1 (Python Worksheet)** or **Option 3 (CLI Script)** instead.
- **Database & Schema Context**: In Container Runtime or headless environments, active database and schema context may not be automatically inherited. If needed, explicitly set `OUTPUT_DATABASE` and `OUTPUT_SCHEMA` in the configuration cell below.

### How to Run
1. Review or adjust the configuration cell below (`LOOKBACK_DAYS = 14` by default; set `OUTPUT_DATABASE` / `OUTPUT_SCHEMA` if needed).
2. Click **Run All** (or run each cell sequentially).
3. At the end of the notebook, click the pre-signed download URL or use the Snowsight Stage UI to download `snowflake_assessment_tco.zip`.
"""

    cell_2_config = LICENSE_HEADER_HASH + """# =====================================================================
# Configuration Parameters
# =====================================================================

# Historical lookback window in days (min: 1, max: 365)
LOOKBACK_DAYS = 14

# Output database and schema where the staging area will be located.
# If None, automatically falls back to CURRENT_DATABASE() and CURRENT_SCHEMA().
# Set these explicitly if running in environments without an active database context.
OUTPUT_DATABASE = None
OUTPUT_SCHEMA = None

# Output Snowflake stage name where the zip archive will be stored
OUTPUT_STAGE = "BQMS_TCO_DISCOVERY_STAGE"

# Name of the output zip package
ZIP_NAME = "snowflake_assessment_tco.zip"

# Enable verbose output (detailed configuration, run ID, and step logging)
VERBOSE = False

# Tool version
TOOL_VERSION = "1.0.0"

# Log message helper for verbose mode
def log(msg: str) -> None:
    if VERBOSE:
        print(msg)

# Validate configuration
if not isinstance(LOOKBACK_DAYS, int) or LOOKBACK_DAYS < 1 or LOOKBACK_DAYS > 365:
    raise ValueError(f"LOOKBACK_DAYS must be an integer between 1 and 365 (got: {LOOKBACK_DAYS})")

log(f"[CONFIG] Lookback Days:   {LOOKBACK_DAYS}")
log(f"[CONFIG] Output Database: {OUTPUT_DATABASE or '(auto-detect)'}")
log(f"[CONFIG] Output Schema:   {OUTPUT_SCHEMA or '(auto-detect)'}")
log(f"[CONFIG] Output Stage:    {OUTPUT_STAGE}")
log(f"[CONFIG] Zip Package:     {ZIP_NAME}")
log(f"[CONFIG] Tool Version:    {TOOL_VERSION}")
"""

    cell_3_setup = (
        """# =====================================================================
# Snowflake Session & Environment Setup
# =====================================================================
import datetime
import decimal
import re
import time
from snowflake.snowpark.context import get_active_session

"""
        + SHARED_QUOTE_IDENT_CODE
        + """


# Get active Snowpark session and underlying DB-API cursor
session = get_active_session()
cur = None
try:
    if hasattr(session, "connection"):
        cur = session.connection.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
except Exception:
    cur = None

# Configure session parameters (UTC timezone and statement timeout)
try:
    if cur is not None:
        cur.execute("ALTER SESSION SET TIMEZONE='UTC', STATEMENT_TIMEOUT_IN_SECONDS=3600;")
    else:
        session.sql("ALTER SESSION SET TIMEZONE='UTC', STATEMENT_TIMEOUT_IN_SECONDS=3600;").collect()
except Exception as e:
    print(f"[WARN] Failed to set session parameters: {e}")

# Resolve and validate database and schema context
target_db = OUTPUT_DATABASE
target_schema = OUTPUT_SCHEMA
try:
    if cur is not None:
        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
        db_schema_row = cur.fetchone()
    else:
        db_schema_res = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();").collect()
        db_schema_row = db_schema_res[0] if db_schema_res else None
    if not target_db and db_schema_row and db_schema_row[0]:
        target_db = str(db_schema_row[0])
    if not target_schema and db_schema_row and db_schema_row[1]:
        target_schema = str(db_schema_row[1])
except Exception as e:
    print(f"[WARN] Failed to resolve CURRENT_DATABASE() / CURRENT_SCHEMA(): {e}")

if not target_db or not target_schema:
    raise RuntimeError(
        "No database/schema selected. Set OUTPUT_DATABASE and OUTPUT_SCHEMA at the top of the file/notebook."
    )

target_stage = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(OUTPUT_STAGE)}"
log(f"[INFO] Resolved Target Stage: @{target_stage}")

# Pre-flight check: capture account region
account_region = "UNKNOWN"
try:
    if cur is not None:
        cur.execute("SELECT CURRENT_REGION();")
        region_row = cur.fetchone()
    else:
        region_res = session.sql("SELECT CURRENT_REGION();").collect()
        region_row = region_res[0] if region_res else None
    if region_row and region_row[0]:
        account_region = str(region_row[0])
except Exception as e:
    print(f"[WARN] Failed to get CURRENT_REGION(): {e}")

if account_region == "UNKNOWN":
    print("[WARN] Account region is UNKNOWN")
else:
    log(f"[INFO] Connected to Snowflake. Account Region: {account_region}")

# Pre-flight check: probe SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
access_history_available = True
try:
    if cur is not None:
        cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;")
        cur.fetchone()
    else:
        session.sql("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;").collect()
    log("[INFO] SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY is available.")
except Exception:
    access_history_available = False
    print("[WARN] SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY is inaccessible (e.g. Snowflake Standard Edition or missing GOVERNANCE_VIEWER role).")
    print("[WARN] Falling back to IsMultiObject = FALSE in warehouses_workload_structure_mds.")

# Initialize run tracking
start_time_epoch = time.time()
start_time_ms = int(start_time_epoch * 1000)
start_dt_utc = datetime.datetime.now(datetime.timezone.utc)
start_time_iso = start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
run_date = start_dt_utc.strftime("%Y-%m-%d")
run_id = f"snowflake-discovery-{start_dt_utc.strftime('%Y%m%d%H%M%S')}"

log(f"[INFO] Run ID:          {run_id}")
log(f"[INFO] Start Time (UTC): {start_time_iso}")
"""
    )


    sql_constants = format_sql_constants(templates)

    cell_4_extraction = f"""# =====================================================================
# Extraction Tasks & Query Execution
# =====================================================================
import csv
import datetime
import decimal
import os
import re
import tempfile

{sql_constants}


{SHARED_RENDER_SQL_CODE}


{SHARED_FORMAT_VALUE_CODE}


temp_dir = tempfile.mkdtemp(prefix="snowflake_tco_")
task_details = []
errors = []
succeeded_count = 0
failed_count = 0

log(f"[INFO] Staging extracted data in: {{temp_dir}}\\n")

for sql_file, output_csv, raw_sql, desc in EXTRACTION_TASKS:
    output_path = os.path.join(temp_dir, output_csv)
    q_start = time.time()

    try:
        rendered_sql = render_sql(
            raw_sql,
            lookback_days=LOOKBACK_DAYS,
            access_history_available=access_history_available,
        )

        row_count = 0
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\\n")
            if cur is not None:
                try:
                    cur.execute(rendered_sql)
                    headers = [col[0] for col in cur.description]
                    writer.writerow(headers)
                    for row in cur:
                        writer.writerow([format_value(v) for v in row])
                        row_count += 1
                except Exception:
                    df = session.sql(rendered_sql)
                    headers = df.columns
                    writer.writerow(headers)
                    for row in df.collect():
                        writer.writerow([format_value(v) for v in row])
                        row_count += 1
            else:
                df = session.sql(rendered_sql)
                headers = df.columns
                writer.writerow(headers)
                for row in df.collect():
                    writer.writerow([format_value(v) for v in row])
                    row_count += 1

        elapsed = int(time.time() - q_start)
        succeeded_count += 1
        task_details.append({{
            "name": output_csv,
            "type": "REQUIRED",
            "status": "SUCCEEDED",
            "duration_seconds": elapsed,
            "row_count": row_count,
        }})
        print(f"{{output_csv}}  {{row_count}} rows  {{elapsed}}s")

    except Exception as e:
        elapsed = int(time.time() - q_start)
        failed_count += 1
        err_msg = str(e)
        errors.append(f"{{output_csv}}: {{err_msg}}")
        task_details.append({{
            "name": output_csv,
            "type": "REQUIRED",
            "status": "FAILED",
            "duration_seconds": elapsed,
            "error": err_msg,
        }})
        print(f"{{output_csv}}  FAILED ({{elapsed}}s): {{err_msg}}")
"""

    cell_5_packaging = """# =====================================================================
# Metadata Descriptor, Telemetry & Archive Packaging
# =====================================================================
import json
import zipfile

# 1. Generate BQMS Assessment metadata descriptor (compilerworks-metadata.yaml)
metadata_yaml_path = os.path.join(temp_dir, "compilerworks-metadata.yaml")
metadata_content = f\"\"\"format: "{ZIP_NAME}"
timestamp: {start_time_ms}
product:
  version: "migration-discovery-scripts:{TOOL_VERSION}"
  arguments: "ConnectorArguments{{connector=snowflake-mds, assessment=true}}"
\"\"\"
with open(metadata_yaml_path, "w", encoding="utf-8") as f:
    f.write(metadata_content)

task_details.append({
    "name": "compilerworks-metadata.yaml",
    "type": "REQUIRED",
    "status": "SUCCEEDED",
    "duration_seconds": 0,
})

# 2. Generate BQMS Client Telemetry (dumper-telemetry.jsonl)
end_dt_utc = datetime.datetime.now(datetime.timezone.utc)
end_time_iso = end_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
total_seconds = int(time.time() - start_time_epoch)
duration_mins = (total_seconds + 59) // 60
overall_status = "SUCCESS" if failed_count == 0 else "FAILED"
error_summary = "; ".join(errors)

telemetry_record = {
    "id": run_id,
    "eventType": "DUMPER_RUN_METRICS",
    "dumperRunStartClientTime": start_time_iso,
    "dumperMetadata": {
        "version": TOOL_VERSION,
        "gitCommit": "migration-discovery-scripts",
        "buildDate": run_date,
    },
    "payload": [
        {
            "id": "snowflake-tco-discovery",
            "measureStartTime": start_time_iso,
            "measureEndTime": end_time_iso,
            "started_at": start_time_iso,
            "completed_at": end_time_iso,
            "runDurationInMinutes": duration_mins,
            "duration_seconds": total_seconds,
            "overallStatus": overall_status,
            "status": overall_status,
            "failed_tasks": failed_count,
            "error_summary": error_summary,
            "taskExecutionSummary": [
                {"count": succeeded_count, "status": "SUCCEEDED"},
                {"count": failed_count, "status": "FAILED"},
            ],
            "taskDetailedSummary": task_details,
            "argumentSummary": {
                "connector": "snowflake-mds",
                "assessment": True,
                "account_region": account_region,
                "lookback_days": LOOKBACK_DAYS,
                "access_history_available": access_history_available,
                "extraction_method": "snowflake-notebook",
            },
        }
    ],
}

telemetry_path = os.path.join(temp_dir, "dumper-telemetry.jsonl")
with open(telemetry_path, "w", encoding="utf-8") as f:
    f.write(json.dumps(telemetry_record, separators=(",", ":")) + "\\n")

task_details.append({
    "name": "dumper-telemetry.jsonl",
    "type": "REQUIRED",
    "status": "SUCCEEDED",
    "duration_seconds": 0,
})

log("[INFO] Created compilerworks-metadata.yaml")
log("[INFO] Created dumper-telemetry.jsonl")

# Abort packaging if any task failed
if failed_count > 0:
    print(f"\\n[ERROR] {failed_count} extraction task(s) failed. Telemetry recorded. Aborting zip packaging.")
    for err in errors:
        print(f"  - {err}")
    raise RuntimeError(f"{failed_count} extraction task(s) failed: {error_summary}")

# Package into ZIP archive
local_zip_path = os.path.join(temp_dir, ZIP_NAME)
with zipfile.ZipFile(local_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for fname in [
        "warehouses_workload_structure_mds.csv",
        "warehouse_metering_history_mds.csv",
        "table_storage_metrics_mds.csv",
        "compilerworks-metadata.yaml",
        "dumper-telemetry.jsonl",
    ]:
        fpath = os.path.join(temp_dir, fname)
        zf.write(fpath, arcname=fname)

zip_size_bytes = os.path.getsize(local_zip_path)
log(f"[INFO] Successfully packaged {ZIP_NAME} ({zip_size_bytes:,} bytes)")
"""

    cell_6_upload = """# =====================================================================
# Stage Upload & Download Generation
# =====================================================================

# Ensure target stage is resolved
if "target_stage" not in globals() or not target_stage:
    target_db = globals().get("OUTPUT_DATABASE")
    target_schema = globals().get("OUTPUT_SCHEMA")
    try:
        if cur is not None:
            cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
            db_schema_row = cur.fetchone()
        else:
            db_schema_res = session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();").collect()
            db_schema_row = db_schema_res[0] if db_schema_res else None
        if not target_db and db_schema_row and db_schema_row[0]:
            target_db = str(db_schema_row[0])
        if not target_schema and db_schema_row and db_schema_row[1]:
            target_schema = str(db_schema_row[1])
    except Exception:
        pass
    if not target_db or not target_schema:
        raise RuntimeError("No database/schema selected. Set OUTPUT_DATABASE and OUTPUT_SCHEMA at the top of the file/notebook.")
    target_stage = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(OUTPUT_STAGE)}"

# Ensure the stage exists with SSE encryption (required for presigned URL downloads)
create_stage_sql = f"CREATE STAGE IF NOT EXISTS {target_stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
if cur is not None:
    cur.execute(create_stage_sql)
else:
    session.sql(create_stage_sql).collect()

# Upload the zip package to the internal stage
stage_target = f"@{target_stage}"
log(f"[INFO] Uploading {ZIP_NAME} to {stage_target}...")
put_result = session.file.put(local_zip_path, stage_target, auto_compress=False, overwrite=True)
log("[INFO] Stage upload complete.")

# Generate pre-signed download URL (valid for 3600 seconds / 1 hour)
presigned_sql = f"SELECT GET_PRESIGNED_URL(@{target_stage}, '{ZIP_NAME}', 3600);"
if cur is not None:
    cur.execute(presigned_sql)
    presigned_row = cur.fetchone()
else:
    presigned_res = session.sql(presigned_sql).collect()
    presigned_row = presigned_res[0] if presigned_res else None
presigned_url = presigned_row[0] if presigned_row and presigned_row[0] else "Unavailable"

print(f"Download URL (valid 1h): {presigned_url}")
print(f"Snowsight stage path:   @{target_db}.{target_schema}.{OUTPUT_STAGE}/{ZIP_NAME}")

if VERBOSE:
    print(f"\\nArchive Size:     {zip_size_bytes:,} bytes")
    print(f"Stage Location:   @{target_db}.{target_schema}.{OUTPUT_STAGE}/{ZIP_NAME}")
    print("Snowsight Navigation: Data -> Databases -> "
          f"{target_db} -> {target_schema} -> Stages -> {OUTPUT_STAGE} -> {ZIP_NAME}")
    print("\\nTask Execution Summary:")
    print(f"{'Task Name':<38} {'Status':<12} {'Rows':<10} {'Duration':<10}")
    print("-" * 72)
    for task in task_details:
        t_name = task["name"]
        t_status = task["status"]
        t_rows = str(task.get("row_count", "-"))
        t_dur = f"{task['duration_seconds']}s"
        print(f"{t_name:<38} {t_status:<12} {t_rows:<10} {t_dur:<10}")
    print("-" * 72)
"""

    cell_7_markdown = """## 📤 Next Steps

Once you have downloaded `snowflake_assessment_tco.zip`:

1. **Upload to Google Cloud Storage**:
   ```bash
   gcloud storage cp snowflake_assessment_tco.zip gs://<your-assessment-bucket>/
   ```

2. **Create a Migration Assessment**:
   - In the **Google Cloud Console**, navigate to **BigQuery → Migration Assessment**.
   - Click **Create Assessment**.
   - Select **Snowflake** as the data source.
   - Point the assessment to your Cloud Storage bucket path (`gs://<your-assessment-bucket>/`).
   - Run the assessment to generate comprehensive TCO and workload migration recommendations.
"""

    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": cell_1_markdown.splitlines(keepends=True),
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_2_config.splitlines(keepends=True),
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_3_setup.splitlines(keepends=True),
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_4_extraction.splitlines(keepends=True),
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_5_packaging.splitlines(keepends=True),
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": cell_6_upload.splitlines(keepends=True),
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": cell_7_markdown.splitlines(keepends=True),
        },
    ]

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return notebook


def generate_notebook_json(templates: dict[str, str]) -> str:
    """Generates the formatted JSON string for the notebook."""
    nb_dict = build_notebook_dict(templates)
    return json.dumps(nb_dict, indent=2) + "\n"


# =====================================================================
# Python Worksheet Generator
# =====================================================================

def generate_python_handler(templates: dict[str, str], extraction_method: str = "snowflake-worksheet") -> str:
    """Generates the core Python script with def main(session) for worksheet."""
    sql_constants = format_sql_constants(templates)

    return f'''import csv
import datetime
import decimal
import json
import os
import re
import tempfile
import time
import zipfile

# In Snowflake environment, snowflake.snowpark is automatically available
try:
    import snowflake.snowpark as snowpark
except ImportError:
    snowpark = None

# =====================================================================
# Configuration Parameters
# =====================================================================

# Historical lookback window in days (min: 1, max: 365)
LOOKBACK_DAYS = 14

# Output database and schema where the staging area will be located.
# If None, automatically falls back to CURRENT_DATABASE() and CURRENT_SCHEMA().
# Set these explicitly if running in environments without an active database context
# (such as Snowsight Workspaces Container Runtime or automated scripts).
OUTPUT_DATABASE = None
OUTPUT_SCHEMA = None

# Output Snowflake stage name where the zip archive will be stored
OUTPUT_STAGE = "BQMS_TCO_DISCOVERY_STAGE"

# Name of the output zip package
ZIP_NAME = "snowflake_assessment_tco.zip"

# Tool version
TOOL_VERSION = "1.0.0"

# Module-level result cache to prevent duplicate execution when the worksheet
# is executed as a script and main(session) is also called by the UI handler.
_RESULT = None

{sql_constants}


{SHARED_RENDER_SQL_CODE}


{SHARED_FORMAT_VALUE_CODE}


{SHARED_QUOTE_IDENT_CODE}


def main(session):
    """Main execution handler for Snowflake discovery and TCO extraction."""
    global _RESULT
    if _RESULT is not None:
        return _RESULT

    # Validate configuration
    if not isinstance(LOOKBACK_DAYS, int) or LOOKBACK_DAYS < 1 or LOOKBACK_DAYS > 365:
        raise ValueError(f"LOOKBACK_DAYS must be an integer between 1 and 365 (got: {{LOOKBACK_DAYS}})")

    # Setup session & cursor with fallback
    cur = None
    try:
        if hasattr(session, "connection"):
            cur = session.connection.cursor()
            cur.execute("SELECT 1;")
            cur.fetchone()
    except Exception:
        cur = None

    # Resolve database and schema context for output stage
    target_db = OUTPUT_DATABASE
    target_schema = OUTPUT_SCHEMA
    if not target_db or not target_schema:
        try:
            db_schema_sql = "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();"
            if cur is not None:
                cur.execute(db_schema_sql)
                db_schema_row = cur.fetchone()
            else:
                db_schema_res = session.sql(db_schema_sql).collect()
                db_schema_row = db_schema_res[0] if db_schema_res else None
            if not target_db and db_schema_row and db_schema_row[0]:
                target_db = str(db_schema_row[0])
            if not target_schema and db_schema_row and db_schema_row[1]:
                target_schema = str(db_schema_row[1])
        except Exception:
            pass

    if not target_db or not target_schema:
        _RESULT = session.create_dataframe(
            [("STAGE_CONTEXT", "FAILED", "No database/schema selected. Set OUTPUT_DATABASE and OUTPUT_SCHEMA at the top of the file/notebook.")],
            schema=["STEP", "STATUS", "DETAIL"],
        )
        return _RESULT

    target_stage = f"{{quote_ident(target_db)}}.{{quote_ident(target_schema)}}.{{quote_ident(OUTPUT_STAGE)}}"

    # Configure session parameters (UTC timezone and statement timeout)
    try:
        if cur is not None:
            cur.execute("ALTER SESSION SET TIMEZONE='UTC', STATEMENT_TIMEOUT_IN_SECONDS=3600;")
        else:
            session.sql("ALTER SESSION SET TIMEZONE='UTC', STATEMENT_TIMEOUT_IN_SECONDS=3600;").collect()
    except Exception:
        pass

    # Pre-flight check: capture account region
    account_region = "UNKNOWN"
    try:
        if cur is not None:
            cur.execute("SELECT CURRENT_REGION();")
            region_row = cur.fetchone()
        else:
            region_res = session.sql("SELECT CURRENT_REGION();").collect()
            region_row = region_res[0] if region_res else None
        if region_row and region_row[0]:
            account_region = str(region_row[0])
    except Exception:
        pass

    # Pre-flight check: probe SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    access_history_available = True
    try:
        if cur is not None:
            cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;")
            cur.fetchone()
        else:
            session.sql("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;").collect()
    except Exception:
        access_history_available = False

    # Initialize run tracking
    start_time_epoch = time.time()
    start_time_ms = int(start_time_epoch * 1000)
    start_dt_utc = datetime.datetime.now(datetime.timezone.utc)
    start_time_iso = start_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = start_dt_utc.strftime("%Y-%m-%d")
    run_id = f"snowflake-discovery-{{start_dt_utc.strftime('%Y%m%d%H%M%S')}}"

    temp_dir = tempfile.mkdtemp(prefix="snowflake_tco_")
    task_details = []
    errors = []
    succeeded_count = 0
    failed_count = 0

    # Execute extraction tasks
    for sql_file, output_csv, raw_sql, desc in EXTRACTION_TASKS:
        output_path = os.path.join(temp_dir, output_csv)
        q_start = time.time()
        try:
            rendered_sql = render_sql(
                raw_sql,
                lookback_days=LOOKBACK_DAYS,
                access_history_available=access_history_available,
            )

            row_count = 0
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\\n")
                if cur is not None:
                    try:
                        cur.execute(rendered_sql)
                        headers = [col[0] for col in cur.description]
                        writer.writerow(headers)
                        for row in cur:
                            writer.writerow([format_value(v) for v in row])
                            row_count += 1
                    except Exception:
                        df = session.sql(rendered_sql)
                        headers = df.columns
                        writer.writerow(headers)
                        for row in df.collect():
                            writer.writerow([format_value(v) for v in row])
                            row_count += 1
                else:
                    df = session.sql(rendered_sql)
                    headers = df.columns
                    writer.writerow(headers)
                    for row in df.collect():
                        writer.writerow([format_value(v) for v in row])
                        row_count += 1

            elapsed = int(time.time() - q_start)
            succeeded_count += 1
            task_details.append({{
                "name": output_csv,
                "type": "REQUIRED",
                "status": "SUCCEEDED",
                "duration_seconds": elapsed,
                "row_count": row_count,
            }})

        except Exception as e:
            elapsed = int(time.time() - q_start)
            failed_count += 1
            err_msg = str(e)
            errors.append(f"{{output_csv}}: {{err_msg}}")
            task_details.append({{
                "name": output_csv,
                "type": "REQUIRED",
                "status": "FAILED",
                "duration_seconds": elapsed,
                "error": err_msg,
            }})

    # Generate metadata descriptor (compilerworks-metadata.yaml)
    metadata_yaml_path = os.path.join(temp_dir, "compilerworks-metadata.yaml")
    metadata_content = f"""format: "{{ZIP_NAME}}"
timestamp: {{start_time_ms}}
product:
  version: "migration-discovery-scripts:{{TOOL_VERSION}}"
  arguments: "ConnectorArguments{{{{connector=snowflake-mds, assessment=true}}}}"
"""
    with open(metadata_yaml_path, "w", encoding="utf-8") as f:
        f.write(metadata_content)

    task_details.append({{
        "name": "compilerworks-metadata.yaml",
        "type": "REQUIRED",
        "status": "SUCCEEDED",
        "duration_seconds": 0,
    }})

    # Generate telemetry (dumper-telemetry.jsonl)
    end_dt_utc = datetime.datetime.now(datetime.timezone.utc)
    end_time_iso = end_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    total_seconds = int(time.time() - start_time_epoch)
    duration_mins = (total_seconds + 59) // 60
    overall_status = "SUCCESS" if failed_count == 0 else "FAILED"
    error_summary = "; ".join(errors)

    telemetry_record = {{
        "id": run_id,
        "eventType": "DUMPER_RUN_METRICS",
        "dumperRunStartClientTime": start_time_iso,
        "dumperMetadata": {{
            "version": TOOL_VERSION,
            "gitCommit": "migration-discovery-scripts",
            "buildDate": run_date,
        }},
        "payload": [
            {{
                "id": "snowflake-tco-discovery",
                "measureStartTime": start_time_iso,
                "measureEndTime": end_time_iso,
                "started_at": start_time_iso,
                "completed_at": end_time_iso,
                "runDurationInMinutes": duration_mins,
                "duration_seconds": total_seconds,
                "overallStatus": overall_status,
                "status": overall_status,
                "failed_tasks": failed_count,
                "error_summary": error_summary,
                "taskExecutionSummary": [
                    {{"count": succeeded_count, "status": "SUCCEEDED"}},
                    {{"count": failed_count, "status": "FAILED"}},
                ],
                "taskDetailedSummary": task_details,
                "argumentSummary": {{
                    "connector": "snowflake-mds",
                    "assessment": True,
                    "account_region": account_region,
                    "lookback_days": LOOKBACK_DAYS,
                    "access_history_available": access_history_available,
                    "extraction_method": "{extraction_method}",
                }},
            }}
        ],
    }}

    telemetry_path = os.path.join(temp_dir, "dumper-telemetry.jsonl")
    with open(telemetry_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(telemetry_record, separators=(",", ":")) + "\\n")

    task_details.append({{
        "name": "dumper-telemetry.jsonl",
        "type": "REQUIRED",
        "status": "SUCCEEDED",
        "duration_seconds": 0,
    }})

    # Build summary rows table: columns = STEP, STATUS, DETAIL
    summary_rows = []

    # 1. One row per CSV extraction task (keep any failures as FAILED rows)
    for task in task_details:
        t_name = task["name"]
        t_status = task["status"]
        if t_name.endswith(".csv"):
            if t_status == "SUCCEEDED":
                summary_rows.append((t_name, t_status, f"{{task.get('row_count', 0):,}} rows, {{task['duration_seconds']}}s"))
            else:
                summary_rows.append((t_name, t_status, f"Error: {{task.get('error', 'Unknown error')}} ({{task['duration_seconds']}}s)"))
        elif t_status == "FAILED":
            summary_rows.append((t_name, t_status, f"Error: {{task.get('error', 'Unknown error')}}"))

    # 2. Optional WARN rows: only when something degrades
    if not access_history_available:
        summary_rows.append(("access_history", "WARN", "Inaccessible (e.g. Standard Edition); falling back to IsMultiObject = FALSE"))
    if account_region == "UNKNOWN":
        summary_rows.append(("region", "WARN", "Account region could not be determined"))

    # 3. Packaging & Staging (only executed if all extraction tasks succeeded)
    if failed_count == 0:
        local_zip_path = os.path.join(temp_dir, ZIP_NAME)
        with zipfile.ZipFile(local_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fname in [
                "warehouses_workload_structure_mds.csv",
                "warehouse_metering_history_mds.csv",
                "table_storage_metrics_mds.csv",
                "compilerworks-metadata.yaml",
                "dumper-telemetry.jsonl",
            ]:
                fpath = os.path.join(temp_dir, fname)
                zf.write(fpath, arcname=fname)
        zip_size_bytes = os.path.getsize(local_zip_path)

        create_stage_sql = f"CREATE STAGE IF NOT EXISTS {{target_stage}} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
        if cur is not None:
            cur.execute(create_stage_sql)
        else:
            session.sql(create_stage_sql).collect()

        session.file.put(local_zip_path, f"@{{target_stage}}", auto_compress=False, overwrite=True)

        presigned_sql = f"SELECT GET_PRESIGNED_URL(@{{target_stage}}, '{{ZIP_NAME}}', 3600);"
        if cur is not None:
            cur.execute(presigned_sql)
            presigned_row = cur.fetchone()
        else:
            presigned_res = session.sql(presigned_sql).collect()
            presigned_row = presigned_res[0] if presigned_res else None
        presigned_url = presigned_row[0] if presigned_row and presigned_row[0] else "Unavailable"

        summary_rows.extend([
            ("DOWNLOAD_URL", "SUCCEEDED", presigned_url),
            ("SNOWSIGHT_PATH", "INFO", f"Data -> Databases -> {{target_db}} -> {{target_schema}} -> Stages -> {{OUTPUT_STAGE}} -> {{ZIP_NAME}}"),
        ])
    else:
        summary_rows.extend([
            ("DOWNLOAD_URL", "SKIPPED", "No package uploaded"),
            ("SNOWSIGHT_PATH", "SKIPPED", "No package uploaded"),
        ])

    _RESULT = session.create_dataframe(summary_rows, schema=["STEP", "STATUS", "DETAIL"])
    return _RESULT


if __name__ == "__main__":
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    result = main(session)
    if hasattr(result, "show"):
        result.show()
    elif hasattr(result, "collect"):
        for r in result.collect():
            print(r)
'''


def generate_worksheet_py(templates: dict[str, str]) -> str:
    """Generates snowflake_tco_discovery_worksheet.py for Snowsight Python worksheets."""
    header = f'''{LICENSE_HEADER_HASH}# =====================================================================
# Snowflake Discovery & TCO Extraction for BigQuery Migration
# Snowsight Python Worksheet
# =====================================================================
# INSTRUCTIONS:
#
# Option A: Legacy Snowsight UI
# 1. Open Snowsight -> Worksheets -> '+' -> 'Python Worksheet'.
# 2. Select an active Database, Schema, and Warehouse in the context selector.
# 3. In worksheet Settings (top-right gear or settings panel):
#    - Handler: main
#    - Return type: Table()
# 4. Review or adjust LOOKBACK_DAYS below if needed (default: 14).
# 5. Click 'Run'.
#
# Option B: New Snowsight Workspaces UI
# 1. Open Snowsight -> Workspaces -> '+' -> 'Python Worksheet' (or run as script).
# 2. If the workspace does not have an active database/schema context,
#    set OUTPUT_DATABASE and OUTPUT_SCHEMA below to an existing database/schema.
# 3. Note: Workspaces using Container Runtime require compute-pool quota.
#    If execution fails due to compute pool or node quota limits,
#    switch to the legacy warehouse runtime or use Option 3 (CLI script).
# 4. Click 'Run' (runs main(session) or executes as script via __main__).
#
# 5. Review the returned summary table for the pre-signed download URL
#    and Snowsight Stage download path.
#
# REQUIRED PRIVILEGES:
# - IMPORTED PRIVILEGES on database SNOWFLAKE (to read SNOWFLAKE.ACCOUNT_USAGE).
# - USAGE on the active Database, Schema, and Virtual Warehouse.
# - CREATE STAGE on the active Schema (or pre-existing SSE stage BQMS_TCO_DISCOVERY_STAGE).
# ====================================================================='''

    body = generate_python_handler(templates, extraction_method="snowflake-worksheet")
    return f"{header}\n\n{body}\n"


# =====================================================================
# Main CLI & Validation
# =====================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generates native Snowflake discovery artifacts from SQL templates."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if committed artifacts match generated files without modifying.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_NOTEBOOK,
        help=f"Path to output notebook (default: {DEFAULT_OUTPUT_NOTEBOOK})",
    )
    parser.add_argument(
        "--worksheet-path",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_WORKSHEET,
        help=f"Path to output Python worksheet (default: {DEFAULT_OUTPUT_WORKSHEET})",
    )
    args = parser.parse_args()

    templates = load_sql_templates()
    generated_notebook = generate_notebook_json(templates)
    generated_worksheet = generate_worksheet_py(templates)

    targets = [
        ("Notebook", args.output, generated_notebook),
        ("Worksheet", args.worksheet_path, generated_worksheet),
    ]

    if args.check:
        drift_errors = []
        for name, path, expected in targets:
            if not path.is_file():
                drift_errors.append(f"Missing file: '{path}'")
                continue
            actual = path.read_text(encoding="utf-8")
            if actual != expected:
                drift_errors.append(
                    f"Drift detected in {name} '{path}'. Run 'python3 build_notebook.py' to regenerate."
                )

        if drift_errors:
            print("[CHECK FAILED] Artifacts are out of sync with generator / SQL templates:", file=sys.stderr)
            for err in drift_errors:
                print(f"  - {err}", file=sys.stderr)
            sys.exit(1)

        print("[CHECK PASSED] All native Snowflake discovery artifacts are up to date.")
        sys.exit(0)

    for name, path, content in targets:
        path.write_text(content, encoding="utf-8")
        print(f"[SUCCESS] Generated {name}: {path}")


if __name__ == "__main__":
    main()
