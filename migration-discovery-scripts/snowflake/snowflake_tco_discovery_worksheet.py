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
# =====================================================================
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
# 4. Review or adjust START_DATE and END_DATE below if needed (defaults to last 14 days).
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
# =====================================================================

import csv
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

# Extraction date range (UTC). Format: 'YYYY-MM-DD'. Default: last 14 days.
# Start date is inclusive (00:00:00 UTC), end date is inclusive (through 23:59:59 UTC).
# If None, START_DATE defaults to today - 14 days and END_DATE defaults to today (UTC).
# Minimum retention: 365 days ago. Future dates are not allowed.
START_DATE = None
END_DATE = None

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

# Embedded SQL templates (single source of truth: sql/*.sql)
SQL_WAREHOUSES_WORKLOAD_STRUCTURE = """-- Extraction window (UTC, END_DATE inclusive). Default: last 14 days. For a custom range set e.g.
-- SET START_DATE = '2026-02-01'::DATE;  SET END_DATE = '2026-05-10'::DATE;
SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
SET END_DATE = CURRENT_DATE();
WITH CreditsPerHourPerWarehouse AS (
    SELECT
        WAREHOUSE_NAME AS "WarehouseName",
        DATE_TRUNC('HOUR', START_TIME) AS "AggregateTime",
        SUM(CREDITS_USED_COMPUTE) AS "TotalCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= $START_DATE::TIMESTAMP_TZ AND START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
      AND WAREHOUSE_NAME IS NOT NULL
      AND WAREHOUSE_NAME != 'CLOUD_SERVICES_ONLY'
    GROUP BY 1, 2
    HAVING SUM(CREDITS_USED_COMPUTE) > 0
),
MultiObjectQueries AS (
    SELECT DISTINCT QUERY_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE QUERY_START_TIME >= $START_DATE::TIMESTAMP_TZ AND QUERY_START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
      AND ARRAY_SIZE(DIRECT_OBJECTS_ACCESSED) >= 6
),
FilteredQueryAttribution AS (
    SELECT
        QUERY_ID,
        CREDITS_ATTRIBUTED_COMPUTE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
    WHERE START_TIME >= $START_DATE::TIMESTAMP_TZ AND START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
),
QueryRaw AS (
    SELECT
        QL.QUERY_ID,
        QL.WAREHOUSE_NAME AS "WarehouseName",
        QL.WAREHOUSE_SIZE,
        DATE_TRUNC('HOUR', QL.START_TIME) AS "AggregateTime",
        QL.QUERY_TYPE,
        COALESCE(QL.BYTES_SCANNED, 0) AS "BytesScanned",
        COALESCE(QL.ROWS_PRODUCED, 0) AS "RowsProduced",
        GREATEST(
            CEIL(COALESCE(QL.BYTES_SCANNED, 0) / 1048576.0), 10
        ) AS "QueryBlocks",
        MOQ.QUERY_ID IS NOT NULL AS "IsMultiObject",
        QAH.CREDITS_ATTRIBUTED_COMPUTE AS "RawAttributedCredits",
        COALESCE(
            SUM(QAH.CREDITS_ATTRIBUTED_COMPUTE) OVER (
                PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
            ),
            0.0
        ) AS "HourAttributedCreditsSum",
        GREATEST(COALESCE(QL.EXECUTION_TIME, 0), 1) AS "WeightedExecutionTime",
        SUM(GREATEST(COALESCE(QL.EXECUTION_TIME, 0), 1)) OVER (
            PARTITION BY QL.WAREHOUSE_NAME, DATE_TRUNC('HOUR', QL.START_TIME)
        ) AS "HourExecutionTimeSum",
        COALESCE(Credits."TotalCreditsBilled", 0.0) AS "HourCreditsBilled"
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS QL
    LEFT JOIN CreditsPerHourPerWarehouse AS Credits
        ON QL.WAREHOUSE_NAME = Credits."WarehouseName"
       AND DATE_TRUNC('HOUR', QL.START_TIME) = Credits."AggregateTime"
    LEFT JOIN MultiObjectQueries AS MOQ
        ON QL.QUERY_ID = MOQ.QUERY_ID
    LEFT JOIN FilteredQueryAttribution AS QAH
        ON QL.QUERY_ID = QAH.QUERY_ID
    WHERE QL.WAREHOUSE_NAME IS NOT NULL
      AND QL.WAREHOUSE_SIZE IS NOT NULL
      AND QL.START_TIME >= $START_DATE::TIMESTAMP_TZ AND QL.START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
),
QueryBase AS (
    SELECT
        QR.*,
        IFF(
            QR."HourAttributedCreditsSum" > 0,
            QR."RawAttributedCredits",
            DIV0(QR."WeightedExecutionTime", QR."HourExecutionTimeSum")
                * QR."HourCreditsBilled"
        ) AS "AttributedCredits"
    FROM QueryRaw AS QR
),
QueryClassified AS (
    SELECT
        QB.*,
        (QB.QUERY_TYPE != 'COPY'
         AND DIV0(QB."BytesScanned", GREATEST(QB."RowsProduced", 1)) >= 1073741824
        ) AS "IsFilteringAbove1Gb",
        (QB.QUERY_TYPE != 'COPY'
         AND NOT (
             DIV0(QB."BytesScanned", GREATEST(QB."RowsProduced", 1)) >= 1073741824
         )
         AND (
             (QB."BytesScanned" > 0
              AND DIV0(QB."AttributedCredits", QB."BytesScanned")
                  * 3774873600.0 < 0.003)
             OR QB."AttributedCredits" IS NULL
             OR QB."AttributedCredits" < 0.001
         )) AS "IsLowCost"
    FROM QueryBase AS QB
)
SELECT
    "WarehouseName",
    "AggregateTime",
    BOOLOR_AGG(UPPER(WAREHOUSE_SIZE) = 'ADAPTIVE') AS "IsAdaptiveWarehouse",
    SUM(COALESCE("AttributedCredits", 0.0)) AS "TotalCreditsAttributed",
    SUM(IFF(
        QUERY_TYPE != 'COPY',
        "QueryBlocks", 0
    )) AS "TotalBlocks",
    SUM(IFF(
        QUERY_TYPE = 'COPY',
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForCopy",
    0.0 AS "CreditsForSystem",
    SUM(IFF("IsFilteringAbove1Gb", COALESCE("AttributedCredits", 0.0), 0.0))
        AS "CreditsForFilteringAbove1Gb",
    SUM(IFF("IsFilteringAbove1Gb", "QueryBlocks", 0))
        AS "BlocksForFilteringAbove1Gb",
    SUM(IFF("IsLowCost", COALESCE("AttributedCredits", 0.0), 0.0))
        AS "CreditsForLowCost",
    SUM(IFF("IsLowCost", "QueryBlocks", 0))
        AS "BlocksForLowCost",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForMultiObject",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        COALESCE("AttributedCredits", 0.0), 0.0
    )) AS "CreditsForStandard",
    SUM(IFF(
        QUERY_TYPE != 'COPY'
        AND NOT "IsFilteringAbove1Gb" AND NOT "IsLowCost" AND NOT "IsMultiObject",
        "QueryBlocks", 0
    )) AS "BlocksForStandard"
FROM QueryClassified
GROUP BY 1, 2
HAVING SUM(COALESCE("AttributedCredits", 0.0)) <> 0
    OR SUM(IFF(QUERY_TYPE != 'COPY', "QueryBlocks", 0)) <> 0
ORDER BY "WarehouseName" ASC, "AggregateTime" ASC;
"""

SQL_WAREHOUSE_METERING_HISTORY = """-- Extraction window (UTC, END_DATE inclusive). Default: last 14 days. For a custom range set e.g.
-- SET START_DATE = '2026-02-01'::DATE;  SET END_DATE = '2026-05-10'::DATE;
SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
SET END_DATE = CURRENT_DATE();
SELECT
    START_TIME AS "StartTime",
    END_TIME AS "EndTime",
    WAREHOUSE_ID AS "WarehouseId",
    WAREHOUSE_NAME AS "WarehouseName",
    CREDITS_USED AS "CreditsUsed",
    CREDITS_USED_COMPUTE AS "CreditsUsedCompute",
    CREDITS_USED_CLOUD_SERVICES AS "CreditsUsedCloudServices"
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= $START_DATE::TIMESTAMP_TZ AND START_TIME < DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ
ORDER BY START_TIME ASC;

"""

SQL_TABLE_STORAGE_METRICS = """-- Note: TABLE_STORAGE_METRICS reflects the current storage snapshot and does not support historical date filtering.
SELECT
    'AllTables' AS "TableName",
    COALESCE(SUM(ACTIVE_BYTES), 0) AS "ActiveBytes",
    COALESCE(SUM(TIME_TRAVEL_BYTES), 0) AS "TimeTravelBytes",
    COALESCE(SUM(FAILSAFE_BYTES), 0) AS "FailsafeBytes",
    COALESCE(SUM(RETAINED_FOR_CLONE_BYTES), 0) AS "RetainedForCloneBytes"
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE UPPER(TABLE_CATALOG) != 'SNOWFLAKE'
  AND COALESCE(CATALOG_DROPPED, SCHEMA_DROPPED, TABLE_DROPPED) IS NULL
  AND TABLE_NAME != '_notification_channels';
"""

SQL_REPORT_DATE_RANGE = """-- Extraction window (UTC, END_DATE inclusive). Default: last 14 days. For a custom range set e.g.
-- SET START_DATE = '2026-02-01'::DATE;  SET END_DATE = '2026-05-10'::DATE;
SET START_DATE = DATEADD('day', -14, CURRENT_DATE());
SET END_DATE = CURRENT_DATE();
SELECT
    $START_DATE::TIMESTAMP_TZ AS "StartTime",
    DATEADD('day', 1, $END_DATE)::TIMESTAMP_TZ AS "EndTime";

"""

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
    (
        "report_date_range_mds.sql",
        "report_date_range.csv",
        SQL_REPORT_DATE_RANGE,
        "Report Date Range",
    ),
]


def render_sql(
    raw_sql: str,
    start_date: str,
    end_date: str,
    access_history_available: bool = True,
) -> str:
    """Renders SQL template by removing SET lines and substituting variables."""
    # 1. Strip SET lines case-insensitively
    lines = []
    for line in raw_sql.splitlines():
        if re.match(r"^\s*SET\s+(START_DATE|END_DATE)\s*=.*;\s*$", line, re.IGNORECASE):
            continue
        lines.append(line)
    sql = "\n".join(lines)

    # 2. Substitute variable placeholders
    sql = sql.replace("$START_DATE", f"'{start_date}'::DATE")
    sql = sql.replace("$END_DATE", f"'{end_date}'::DATE")

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
                if re.match(r"^\s*\),", line):
                    skip = False
                continue
            new_lines.append(line)
        sql = "\n".join(new_lines)

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
    if isinstance(val, decimal.Decimal):
        return format(val, "f")
    if isinstance(val, float):
        # Matches SnowSQL FLOAT rendering exactly (10 significant digits,
        # e.g. '1', '0.999999', '1.055555556e-05').
        return format(val, ".10g")
    return str(val)


def quote_ident(ident: str) -> str:
    """Quotes a Snowflake identifier if it contains lowercase or special characters."""
    if not ident or not isinstance(ident, str):
        raise ValueError(f"Invalid identifier: {ident!r}")
    if re.match(r"^[A-Za-z_][A-Za-z0-9_$]*$", ident):
        return ident
    return '"' + ident.replace('"', '""') + '"' 


def main(session):
    """Main execution handler for Snowflake discovery and TCO extraction."""
    global _RESULT
    if _RESULT is not None:
        return _RESULT

    # Resolve and validate date range
    today_utc = datetime.datetime.now(datetime.timezone.utc).date()
    default_end = today_utc
    default_start = today_utc - datetime.timedelta(days=14)

    if START_DATE is None:
        start_d = default_start
        effective_start_date = start_d.isoformat()
    elif not isinstance(START_DATE, str):
        raise ValueError("START_DATE must be a string in 'YYYY-MM-DD' format.")
    else:
        try:
            start_d = datetime.date.fromisoformat(START_DATE)
            effective_start_date = start_d.isoformat()
        except Exception as e:
            raise ValueError(f"Invalid START_DATE format '{START_DATE}': {e}. Expected YYYY-MM-DD.")

    if END_DATE is None:
        end_d = default_end
        effective_end_date = end_d.isoformat()
    elif not isinstance(END_DATE, str):
        raise ValueError("END_DATE must be a string in 'YYYY-MM-DD' format.")
    else:
        try:
            end_d = datetime.date.fromisoformat(END_DATE)
            effective_end_date = end_d.isoformat()
        except Exception as e:
            raise ValueError(f"Invalid END_DATE format '{END_DATE}': {e}. Expected YYYY-MM-DD.")

    if start_d > end_d:
        raise ValueError(f"START_DATE ({effective_start_date}) must be on or before END_DATE ({effective_end_date}).")
    if end_d > today_utc:
        raise ValueError(f"END_DATE ({effective_end_date}) cannot be in the future (today UTC: {today_utc}).")
    min_date = today_utc - datetime.timedelta(days=365)
    if start_d < min_date:
        raise ValueError(f"START_DATE ({effective_start_date}) exceeds Snowflake ACCOUNT_USAGE 365-day retention limit (oldest allowed: {min_date}).")

    end_plus_1_d = end_d + datetime.timedelta(days=1)
    date_range_start = f"{effective_start_date}T00:00:00Z"
    date_range_end = f"{end_plus_1_d.isoformat()}T00:00:00Z"

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

    target_stage = f"{quote_ident(target_db)}.{quote_ident(target_schema)}.{quote_ident(OUTPUT_STAGE)}"

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
    run_id = f"snowflake-discovery-{start_dt_utc.strftime('%Y%m%d%H%M%S')}"

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
                start_date=effective_start_date,
                end_date=effective_end_date,
                access_history_available=access_history_available,
            )

            row_count = 0
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
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
            task_details.append({
                "name": output_csv,
                "type": "REQUIRED",
                "status": "SUCCEEDED",
                "duration_seconds": elapsed,
                "row_count": row_count,
            })

        except Exception as e:
            elapsed = int(time.time() - q_start)
            failed_count += 1
            err_msg = str(e)
            errors.append(f"{output_csv}: {err_msg}")
            task_details.append({
                "name": output_csv,
                "type": "REQUIRED",
                "status": "FAILED",
                "duration_seconds": elapsed,
                "error": err_msg,
            })

    # Generate metadata descriptor (compilerworks-metadata.yaml)
    metadata_yaml_path = os.path.join(temp_dir, "compilerworks-metadata.yaml")
    metadata_content = f"""format: "{ZIP_NAME}"
timestamp: {start_time_ms}
product:
  version: "migration-discovery-scripts:{TOOL_VERSION}"
  arguments: "ConnectorArguments{{connector=snowflake-mds, assessment=true}}"
"""
    with open(metadata_yaml_path, "w", encoding="utf-8") as f:
        f.write(metadata_content)

    task_details.append({
        "name": "compilerworks-metadata.yaml",
        "type": "REQUIRED",
        "status": "SUCCEEDED",
        "duration_seconds": 0,
    })

    # Generate telemetry (dumper-telemetry.jsonl)
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
                    "access_history_available": access_history_available,
                    "extraction_method": "snowflake-worksheet",
                    "date_range_start": date_range_start,
                    "date_range_end": date_range_end,
                },
            }
        ],
    }

    telemetry_path = os.path.join(temp_dir, "dumper-telemetry.jsonl")
    with open(telemetry_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(telemetry_record, separators=(",", ":")) + "\n")

    task_details.append({
        "name": "dumper-telemetry.jsonl",
        "type": "REQUIRED",
        "status": "SUCCEEDED",
        "duration_seconds": 0,
    })

    # Build summary rows table: columns = STEP, STATUS, DETAIL
    summary_rows = []

    # 1. One row per CSV extraction task (keep any failures as FAILED rows)
    for task in task_details:
        t_name = task["name"]
        t_status = task["status"]
        if t_name.endswith(".csv"):
            if t_status == "SUCCEEDED":
                summary_rows.append((t_name, t_status, f"{task.get('row_count', 0):,} rows, {task['duration_seconds']}s"))
            else:
                summary_rows.append((t_name, t_status, f"Error: {task.get('error', 'Unknown error')} ({task['duration_seconds']}s)"))
        elif t_status == "FAILED":
            summary_rows.append((t_name, t_status, f"Error: {task.get('error', 'Unknown error')}"))

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
                "report_date_range.csv",
                "compilerworks-metadata.yaml",
                "dumper-telemetry.jsonl",
            ]:
                fpath = os.path.join(temp_dir, fname)
                zf.write(fpath, arcname=fname)
        zip_size_bytes = os.path.getsize(local_zip_path)

        create_stage_sql = f"CREATE STAGE IF NOT EXISTS {target_stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
        if cur is not None:
            cur.execute(create_stage_sql)
        else:
            session.sql(create_stage_sql).collect()

        session.file.put(local_zip_path, f"@{target_stage}", auto_compress=False, overwrite=True)

        presigned_sql = f"SELECT GET_PRESIGNED_URL(@{target_stage}, '{ZIP_NAME}', 3600);"
        if cur is not None:
            cur.execute(presigned_sql)
            presigned_row = cur.fetchone()
        else:
            presigned_res = session.sql(presigned_sql).collect()
            presigned_row = presigned_res[0] if presigned_res else None
        presigned_url = presigned_row[0] if presigned_row and presigned_row[0] else "Unavailable"

        summary_rows.extend([
            ("DOWNLOAD_URL", "SUCCEEDED", presigned_url),
            ("SNOWSIGHT_PATH", "INFO", f"Data -> Databases -> {target_db} -> {target_schema} -> Stages -> {OUTPUT_STAGE} -> {ZIP_NAME}"),
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

