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
"""Builds snowflake_tco_discovery.ipynb from SQL templates."""

import argparse
import json
import os
import pathlib
import sys

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
SQL_DIR = SCRIPT_DIR / "sql"
DEFAULT_OUTPUT_NOTEBOOK = SCRIPT_DIR / "snowflake_tco_discovery.ipynb"


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

### How to Run
1. Review or adjust the configuration cell below (`LOOKBACK_DAYS = 14` by default).
2. Click **Run All** (or run each cell sequentially).
3. At the end of the notebook, click the pre-signed download URL or use the Snowsight Stage UI to download `snowflake_assessment_tco.zip`.
"""

    cell_2_config = """# =====================================================================
# Configuration Parameters
# =====================================================================

# Historical lookback window in days (min: 1, max: 365)
LOOKBACK_DAYS = 14

# Output Snowflake stage name where the zip archive will be stored
OUTPUT_STAGE = "BQMS_TCO_DISCOVERY_STAGE"

# Name of the output zip package
ZIP_NAME = "snowflake_assessment_tco.zip"

# Tool version
TOOL_VERSION = "1.0.0"

# Validate configuration
if not isinstance(LOOKBACK_DAYS, int) or LOOKBACK_DAYS < 1 or LOOKBACK_DAYS > 365:
    raise ValueError(f"LOOKBACK_DAYS must be an integer between 1 and 365 (got: {LOOKBACK_DAYS})")

print(f"[CONFIG] Lookback Days: {LOOKBACK_DAYS}")
print(f"[CONFIG] Output Stage:  @{OUTPUT_STAGE}")
print(f"[CONFIG] Zip Package:   {ZIP_NAME}")
print(f"[CONFIG] Tool Version:  {TOOL_VERSION}")
"""

    cell_3_setup = """# =====================================================================
# Snowflake Session & Environment Setup
# =====================================================================
import datetime
import decimal
import time
from snowflake.snowpark.context import get_active_session

# Get active Snowpark session and underlying DB-API cursor
session = get_active_session()
cur = session.connection.cursor()

# Configure session parameters (UTC timezone and statement timeout)
cur.execute("ALTER SESSION SET TIMEZONE='UTC', STATEMENT_TIMEOUT_IN_SECONDS=3600;")

# Pre-flight check: capture account region
cur.execute("SELECT CURRENT_REGION();")
region_row = cur.fetchone()
account_region = region_row[0] if region_row and region_row[0] else "UNKNOWN"
print(f"[INFO] Connected to Snowflake. Account Region: {account_region}")

# Pre-flight check: probe SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
access_history_available = True
try:
    cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;")
    cur.fetchone()
    print("[INFO] SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY is available.")
except Exception as e:
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

print(f"[INFO] Run ID:          {run_id}")
print(f"[INFO] Start Time (UTC): {start_time_iso}")
"""

    # Escape triple quotes in raw SQL strings just in case
    escaped_workload = templates["warehouses_workload_structure_mds"].replace('"""', r'\"\"\"')
    escaped_metering = templates["warehouse_metering_history_mds"].replace('"""', r'\"\"\"')
    escaped_storage = templates["table_storage_metrics_mds"].replace('"""', r'\"\"\"')

    cell_4_extraction = f'''# =====================================================================
# Extraction Tasks & Query Execution
# =====================================================================
import csv
import os
import re
import tempfile

# Embedded SQL templates (single source of truth: migration-discovery-scripts/snowflake/sql/*.sql)
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
]


def render_sql(raw_sql: str, lookback_days: int, access_history_available: bool = True) -> str:
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

    return sql


def format_value(val) -> str:
    """Formats values for CSV export matching BQMS Assessment ingestion contracts."""
    if val is None:
        return ""
    if isinstance(val, datetime.datetime):
        if val.tzinfo is not None:
            val = val.astimezone(datetime.timezone.utc)
        return val.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(val, datetime.date):
        return val.isoformat()
    if isinstance(val, (decimal.Decimal, float)):
        return str(val)
    return str(val)


temp_dir = tempfile.mkdtemp(prefix="snowflake_tco_")
task_details = []
errors = []
succeeded_count = 0
failed_count = 0

print(f"[INFO] Staging extracted data in: {{temp_dir}}\\n")

for sql_file, output_csv, raw_sql, desc in EXTRACTION_TASKS:
    output_path = os.path.join(temp_dir, output_csv)
    print(f"[INFO] Extracting {{desc}} ({{output_csv}})... ", end="", flush=True)
    q_start = time.time()

    try:
        rendered_sql = render_sql(
            raw_sql,
            lookback_days=LOOKBACK_DAYS,
            access_history_available=access_history_available,
        )

        cur.execute(rendered_sql)
        headers = [col[0] for col in cur.description]

        row_count = 0
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in cur:
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
        print(f"DONE ({{row_count}} rows, {{elapsed}}s)")

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
        print(f"FAILED ({{elapsed}}s)")
        print(f"       [ERROR] {{err_msg}}")
'''

    cell_5_packaging = """# =====================================================================
# Metadata Descriptor, Telemetry & Archive Packaging
# =====================================================================
import json
import zipfile

# 1. Generate BQMS Assessment metadata descriptor (compilerworks-metadata.yaml)
metadata_yaml_path = os.path.join(temp_dir, "compilerworks-metadata.yaml")
metadata_content = f\"\"\"format: "snowflake_assessment_tco.zip"
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
                "runtime": "snowflake-notebook",
            },
        }
    ],
}

telemetry_path = os.path.join(temp_dir, "dumper-telemetry.jsonl")
with open(telemetry_path, "w", encoding="utf-8") as f:
    f.write(json.dumps(telemetry_record, separators=(",", ":")) + "\\n")

print(f"[INFO] Created compilerworks-metadata.yaml")
print(f"[INFO] Created dumper-telemetry.jsonl")

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
print(f"[INFO] Successfully packaged {ZIP_NAME} ({zip_size_bytes:,} bytes)")
"""

    cell_6_upload = """# =====================================================================
# Stage Upload & Download Generation
# =====================================================================

# Ensure the stage exists with SSE encryption (required for presigned URL downloads)
cur.execute(f"CREATE STAGE IF NOT EXISTS {OUTPUT_STAGE} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');")

# Upload the zip package to the internal stage
stage_target = f"@{OUTPUT_STAGE}"
print(f"[INFO] Uploading {ZIP_NAME} to {stage_target}...")
put_result = session.file.put(local_zip_path, stage_target, auto_compress=False, overwrite=True)
print(f"[INFO] Stage upload complete.")

# Generate pre-signed download URL (valid for 3600 seconds / 1 hour)
cur.execute(f"SELECT GET_PRESIGNED_URL(@{OUTPUT_STAGE}, '{ZIP_NAME}', 3600);")
presigned_row = cur.fetchone()
presigned_url = presigned_row[0] if presigned_row and presigned_row[0] else "Unavailable"

# Get current database and schema for Snowsight navigation instructions
cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
db_schema_row = cur.fetchone()
curr_db = db_schema_row[0] if db_schema_row and db_schema_row[0] else "<CURRENT_DATABASE>"
curr_schema = db_schema_row[1] if db_schema_row and db_schema_row[1] else "<CURRENT_SCHEMA>"

print("\\n" + "=" * 65)
print("  EXTRACTION COMPLETE & PACKAGE STAGED")
print("=" * 65)
print(f"Stage Location:   @{curr_db}.{curr_schema}.{OUTPUT_STAGE}/{ZIP_NAME}")
print(f"Archive Size:     {zip_size_bytes:,} bytes")
print("-" * 65)
print("DOWNLOAD OPTION 1: Pre-signed URL (Valid for 1 Hour):")
print(presigned_url)
print("-" * 65)
print("DOWNLOAD OPTION 2: Snowsight Web UI:")
print(f"1. Open Snowsight -> Data -> Databases")
print(f"2. Navigate to: {curr_db} -> {curr_schema} -> Stages -> {OUTPUT_STAGE}")
print(f"3. Select '{ZIP_NAME}' and click 'Download'")
print("=" * 65 + "\\n")

# Print Summary Table
print("Task Execution Summary:")
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


def main():
    parser = argparse.ArgumentParser(
        description="Generates snowflake_tco_discovery.ipynb from SQL templates."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if committed .ipynb matches generated notebook without modifying.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_NOTEBOOK,
        help=f"Path to output notebook (default: {DEFAULT_OUTPUT_NOTEBOOK})",
    )
    args = parser.parse_args()

    templates = load_sql_templates()
    generated_content = generate_notebook_json(templates)

    if args.check:
        if not args.output.is_file():
            print(f"[CHECK FAILED] Output notebook '{args.output}' does not exist.", file=sys.stderr)
            sys.exit(1)
        current_content = args.output.read_text(encoding="utf-8")
        if current_content != generated_content:
            print(
                f"[CHECK FAILED] Notebook '{args.output}' is out of sync with SQL templates. "
                f"Run 'python3 build_notebook.py' to regenerate.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"[CHECK PASSED] Notebook '{args.output}' is up to date.")
        sys.exit(0)

    args.output.write_text(generated_content, encoding="utf-8")
    print(f"[SUCCESS] Generated notebook: {args.output}")


if __name__ == "__main__":
    main()
