#!/usr/bin/env bash
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
# Snowflake Migration Discovery Scripts (MDS) for BigQuery TCO Assessment
#
# Usage:
#   ./tco_discovery.sh -a <account> -u <user> [-r <role>] [-w <warehouse>] [-s <start_date>] [-e <end_date>] [-o <output_dir>]
#

set -euo pipefail

TOOL_VERSION="1.0.0"
SF_ACCOUNT=""
SF_USER=""
SF_ROLE=""
SF_WAREHOUSE=""
SF_PRIVATE_KEY_PATH=""
START_DATE=""
END_DATE=""
OUTPUT_DIR="./out"
ZIP_NAME="snowflake_assessment_tco.zip"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

usage() {
  cat << EOF
Usage: $0 [OPTIONS]

Required Options:
  -a, --account           Snowflake Account Identifier (e.g. xy12345.us-east-1 or org-account)
  -u, --user              Snowflake Username

Optional Options:
  -r, --role              Snowflake Role (e.g. ACCOUNTADMIN, SYSADMIN)
  -w, --warehouse         Snowflake Virtual Warehouse to execute queries (e.g. COMPUTE_WH)
  -k, --private-key-path  Path to RSA private key file (e.g. rsa_key.p8) for key-pair auth
  -s, --start-date        Window start date in UTC (YYYY-MM-DD, default: 14 days ago)
  -e, --end-date          Window end date in UTC (YYYY-MM-DD, inclusive, default: today)
  -o, --output            Output staging directory (default: ./out)
  -h, --help              Display this help message

Authentication:
  Export SNOWSQL_PWD='<password>' before running, pass -k /path/to/rsa_key.p8,
  or configure credentials in ~/.snowsql/config.

Examples:
  # Default extraction (last 14 days ending today UTC)
  export SNOWSQL_PWD='<your_snowflake_password>'
  $0 -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH

  # Custom date range (Feb 1, 2026 through May 10, 2026 inclusive)
  $0 -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -s 2026-02-01 -e 2026-05-10
EOF
  exit 1
}

# Parse command line options
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -a|--account)          SF_ACCOUNT="$2"; shift 2 ;;
    -u|--user)             SF_USER="$2"; shift 2 ;;
    -r|--role)             SF_ROLE="$2"; shift 2 ;;
    -w|--warehouse)        SF_WAREHOUSE="$2"; shift 2 ;;
    -k|--private-key-path) SF_PRIVATE_KEY_PATH="$2"; shift 2 ;;
    -s|--start-date)       START_DATE="$2"; shift 2 ;;
    -e|--end-date)         END_DATE="$2"; shift 2 ;;
    -o|--output)           OUTPUT_DIR="$2"; shift 2 ;;
    -h|--help)             usage ;;
    *) echo "[ERROR] Unknown option: $1" >&2; usage ;;
  esac
done

if [[ -z "$SF_ACCOUNT" || -z "$SF_USER" ]]; then
  echo "[ERROR] Missing required arguments: --account (-a) and --user (-u) are required." >&2
  usage
fi

if [[ -n "$SF_PRIVATE_KEY_PATH" && ! -f "$SF_PRIVATE_KEY_PATH" ]]; then
  echo "[ERROR] Private key file not found: $SF_PRIVATE_KEY_PATH" >&2
  exit 1
fi

is_valid_date() {
  local dt="$1"
  if [[ ! "$dt" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    return 1
  fi
  local canon=""
  if canon=$(date -u -d "$dt" +%Y-%m-%d 2>/dev/null); then
    [[ "$canon" == "$dt" ]] && return 0
  fi
  if canon=$(date -u -j -f "%Y-%m-%d" "$dt" +%Y-%m-%d 2>/dev/null); then
    [[ "$canon" == "$dt" ]] && return 0
  fi
  return 1
}

TODAY_UTC=$(date -u +%Y-%m-%d)
DEFAULT_START=$(date -u -d "$TODAY_UTC - 14 days" +%Y-%m-%d 2>/dev/null || date -u -v-14d +%Y-%m-%d 2>/dev/null || date -u -j -v-14d -f "%Y-%m-%d" "$TODAY_UTC" +%Y-%m-%d 2>/dev/null)
DEFAULT_END="$TODAY_UTC"

START_DATE="${START_DATE:-$DEFAULT_START}"
END_DATE="${END_DATE:-$DEFAULT_END}"

if ! is_valid_date "$START_DATE"; then
  echo "[ERROR] Invalid date format for --start-date: '$START_DATE'. Expected a valid calendar date in YYYY-MM-DD format." >&2
  exit 1
fi
if ! is_valid_date "$END_DATE"; then
  echo "[ERROR] Invalid date format for --end-date: '$END_DATE'. Expected a valid calendar date in YYYY-MM-DD format." >&2
  exit 1
fi
if [[ "$START_DATE" > "$END_DATE" ]]; then
  echo "[ERROR] Start date ($START_DATE) must be on or before end date ($END_DATE)." >&2
  exit 1
fi
if [[ "$END_DATE" > "$TODAY_UTC" ]]; then
  echo "[ERROR] End date ($END_DATE) cannot be in the future (today UTC: $TODAY_UTC)." >&2
  exit 1
fi
MIN_RETENTION_DATE=$(date -u -d "$TODAY_UTC - 365 days" +%Y-%m-%d 2>/dev/null || date -u -v-365d +%Y-%m-%d 2>/dev/null || date -u -j -v-365d -f "%Y-%m-%d" "$TODAY_UTC" +%Y-%m-%d 2>/dev/null)
if [[ "$START_DATE" < "$MIN_RETENTION_DATE" ]]; then
  echo "[ERROR] Start date ($START_DATE) exceeds Snowflake ACCOUNT_USAGE 365-day retention limit (oldest allowed: $MIN_RETENTION_DATE)." >&2
  exit 1
fi

END_DATE_PLUS_1=$(date -u -d "$END_DATE + 1 day" +%Y-%m-%d 2>/dev/null || date -u -j -v+1d -f "%Y-%m-%d" "$END_DATE" +%Y-%m-%d 2>/dev/null)

DATE_RANGE_START="${START_DATE}T00:00:00Z"
DATE_RANGE_END="${END_DATE_PLUS_1}T00:00:00Z"

# Verify snowsql is installed
if ! command -v snowsql &> /dev/null; then
  echo "[ERROR] 'snowsql' utility was not found in PATH." >&2
  echo "Please install SnowSQL from: https://docs.snowflake.com/en/user-guide/snowsql-install-config" >&2
  exit 1
fi

# Verify zip is installed
if ! command -v zip &> /dev/null; then
  echo "[ERROR] 'zip' command is required but not installed." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

# Session timestamps & tracking
START_TIME_EPOCH=$(date +%s)
START_TIME_MS=$(date +%s%3N 2>/dev/null || true)
if ! [[ "$START_TIME_MS" =~ ^[0-9]+$ ]]; then
  START_TIME_MS="${START_TIME_EPOCH}000"
fi
START_TIME_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
RUN_DATE=$(date -u +"%Y-%m-%d")
RUN_ID="snowflake-discovery-$(date +%Y%m%d%H%M%S)"


# Build SnowSQL command base
SNOWSQL_CMD=(snowsql -a "$SF_ACCOUNT" -u "$SF_USER")
if [[ -n "$SF_ROLE" ]]; then
  SNOWSQL_CMD+=(-r "$SF_ROLE")
fi
if [[ -n "$SF_WAREHOUSE" ]]; then
  SNOWSQL_CMD+=(-w "$SF_WAREHOUSE")
fi
if [[ -n "$SF_PRIVATE_KEY_PATH" ]]; then
  SNOWSQL_CMD+=(--private-key-path "$SF_PRIVATE_KEY_PATH")
fi
SNOWSQL_CMD+=(
  -o exit_on_error=true
  -o output_format=csv
  -o header=true
  -o timing=false
  -o friendly=false
  -o quiet=true
)

SESSION_INIT="ALTER SESSION SET TIMEZONE='UTC', TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"', TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"', TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"', STATEMENT_TIMEOUT_IN_SECONDS=3600;"

# Pre-flight check: Test Snowflake connection, authentication & capture account region
CONN_TEST_QUERY="${SESSION_INIT} SELECT CURRENT_REGION();"
if CONN_OUTPUT=$("${SNOWSQL_CMD[@]}" -q "$CONN_TEST_QUERY" -o output_format=csv -o header=false -o timing=false -o friendly=false -o quiet=false < /dev/null 2>&1); then
  ACCOUNT_REGION=$(echo "$CONN_OUTPUT" | tr -d '\r"' | awk 'NF {last=$0} END {print last}')
  ACCOUNT_REGION="${ACCOUNT_REGION:-UNKNOWN}"
  if [[ "$ACCOUNT_REGION" == "UNKNOWN" ]]; then
    echo "[WARN] Account region is UNKNOWN"
  fi
else
  echo "[ERROR] Could not authenticate or connect to Snowflake." >&2
  echo "[HINT] Ensure SNOWSQL_PWD is exported (export SNOWSQL_PWD='<password>'), -k <private_key_path> is passed, or credentials are set in ~/.snowsql/config." >&2
  echo "$CONN_OUTPUT" >&2
  exit 1
fi

# Pre-flight check on ACCESS_HISTORY
ACCESS_HISTORY_AVAILABLE=true
CHECK_QUERY="${SESSION_INIT} SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;"
if ! "${SNOWSQL_CMD[@]}" -q "$CHECK_QUERY" -o output_format=plain -o header=false -o timing=false -o friendly=false -o quiet=true > /dev/null 2>&1; then
  echo "[WARN] SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY is inaccessible (e.g. Snowflake Standard Edition or missing GOVERNANCE_VIEWER role)."
  echo "[WARN] Falling back to IsMultiObject = FALSE in warehouses_workload_structure_mds.sql."
  ACCESS_HISTORY_AVAILABLE=false
fi

# Extraction tasks: "sql_file:output_csv_name:header_pattern:description"
TASKS=(
  "warehouses_workload_structure_mds.sql:warehouses_workload_structure_mds.csv:^\"?WarehouseName\"?,\"?AggregateTime\"?:Virtual Warehouse Workload Structure"
  "warehouse_metering_history_mds.sql:warehouse_metering_history_mds.csv:^\"?StartTime\"?:Virtual Warehouse Metering History"
  "table_storage_metrics_mds.sql:table_storage_metrics_mds.csv:^\"?TableName\"?,\"?ActiveBytes\"?:Table Storage Breakdown"
  "report_date_range_mds.sql:report_date_range.csv:^\"?StartTime\"?,\"?EndTime\"?$:Report Date Range"
)

TMP_SQL_FILE="$OUTPUT_DIR/.temp_query.sql"
TASK_DETAILS_JSON=""
ERROR_SUMMARY=""
SUCCEEDED_COUNT=0
FAILED_COUNT=0

append_task_detail() {
  local item_json="$1"
  if [[ -z "$TASK_DETAILS_JSON" ]]; then
    TASK_DETAILS_JSON="$item_json"
  else
    TASK_DETAILS_JSON="$TASK_DETAILS_JSON,$item_json"
  fi
}

append_error_summary() {
  local msg="$1"
  if [[ -z "$ERROR_SUMMARY" ]]; then
    ERROR_SUMMARY="$msg"
  else
    ERROR_SUMMARY="$ERROR_SUMMARY; $msg"
  fi
}

clean_csv_header() {
  local csv_file="$1"
  local hdr_pattern="$2"
  if [[ ! -f "$csv_file" ]]; then
    return 1
  fi
  if ! awk -v pat="$hdr_pattern" '
    BEGIN { found = 0 }
    $0 ~ pat { found = 1 }
    found { print }
    END { if (!found) exit 1 }
  ' "$csv_file" > "${csv_file}.tmp"; then
    rm -f "${csv_file}.tmp"
    return 1
  fi
  mv "${csv_file}.tmp" "$csv_file"
}

for task in "${TASKS[@]}"; do
  IFS=":" read -r sql_file output_csv hdr_pattern desc <<< "$task"
  input_path="$SQL_DIR/$sql_file"
  output_path="$OUTPUT_DIR/$output_csv"

  if [[ ! -f "$input_path" ]]; then
    echo "$output_csv  FAILED: SQL template '$input_path' does not exist"
    echo "[ERROR] SQL template '$input_path' does not exist." >&2
    FAILED_COUNT=$((FAILED_COUNT + 1))
    append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"FAILED\",\"duration_seconds\":0,\"error\":\"missing SQL template\"}"
    append_error_summary "$output_csv: missing SQL template"
    continue
  fi

  # Render SQL: strip SET lines (portable across GNU and BSD/macOS sed) and substitute variables
  rendered_sql=$(sed -E \
    -e "s/^[[:space:]]*[Ss][Ee][Tt][[:space:]]+[Ss][Tt][Aa][Rr][Tt]_[Dd][Aa][Tt][Ee][[:space:]]*=.*;[[:space:]]*$//" \
    -e "s/^[[:space:]]*[Ss][Ee][Tt][[:space:]]+[Ee][Nn][Dd]_[Dd][Aa][Tt][Ee][[:space:]]*=.*;[[:space:]]*$//" \
    -e "s|\\\$START_DATE|'${START_DATE}'::DATE|g" \
    -e "s|\\\$END_DATE|'${END_DATE}'::DATE|g" \
    "$input_path")

  # If ACCESS_HISTORY is unavailable, replace MultiObjectQueries query body
  if [[ "$sql_file" == "warehouses_workload_structure_mds.sql" && "$ACCESS_HISTORY_AVAILABLE" == "false" ]]; then
    rendered_sql=$(echo "$rendered_sql" | awk '
      /MultiObjectQueries AS \(/ {
        print "MultiObjectQueries AS ("
        print "    SELECT CAST(NULL AS VARCHAR) AS QUERY_ID WHERE 1=0"
        print "),"
        skip = 1
        next
      }
      skip && /^[[:space:]]*\),/ {
        skip = 0
        next
      }
      !skip { print }
    ')
  fi

  cat <<EOF > "$TMP_SQL_FILE"
${SESSION_INIT}
${rendered_sql}
EOF

  # Execute query via snowsql
  query_start_epoch=$(date +%s)
  if "${SNOWSQL_CMD[@]}" -f "$TMP_SQL_FILE" -o output_file="$output_path" > "$output_path.stdout" 2>&1; then
    query_elapsed_secs=$(( $(date +%s) - query_start_epoch ))
    # If output was sent to stdout instead of output_file, copy it
    if [[ ! -s "$output_path" && -s "$output_path.stdout" ]]; then
      mv "$output_path.stdout" "$output_path"
    else
      rm -f "$output_path.stdout"
    fi

    # Strip any multi-statement preamble and verify expected CSV header is present
    if clean_csv_header "$output_path" "$hdr_pattern"; then
      if [[ -s "$output_path" ]]; then
        row_count=$(($(wc -l < "$output_path") - 1))
        echo "$output_csv  $row_count rows  ${query_elapsed_secs}s"
      else
        row_count=0
        echo "$output_csv  0 rows  ${query_elapsed_secs}s"
      fi
      SUCCEEDED_COUNT=$((SUCCEEDED_COUNT + 1))
      append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"SUCCEEDED\",\"duration_seconds\":$query_elapsed_secs,\"row_count\":$row_count}"
    else
      FAILED_COUNT=$((FAILED_COUNT + 1))
      append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"FAILED\",\"duration_seconds\":$query_elapsed_secs,\"error\":\"missing expected CSV header\"}"
      append_error_summary "$output_csv: missing expected CSV header"
      echo "$output_csv  FAILED (${query_elapsed_secs}s): missing expected CSV header"
      echo "[ERROR] Expected header pattern '$hdr_pattern' not found in $output_csv" >&2
      if [[ -f "$output_path" && -s "$output_path" ]]; then
        echo "[ERROR] Output details:" >&2
        cat "$output_path" >&2
      fi
      rm -f "$output_path" "$output_path.stdout"
    fi
  else
    query_elapsed_secs=$(( $(date +%s) - query_start_epoch ))
    FAILED_COUNT=$((FAILED_COUNT + 1))
    append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"FAILED\",\"duration_seconds\":$query_elapsed_secs,\"error\":\"snowsql execution failure\"}"
    append_error_summary "$output_csv: snowsql execution failure"
    echo "$output_csv  FAILED (${query_elapsed_secs}s): snowsql execution failure"
    echo "[ERROR] Failed executing $sql_file" >&2
    if [[ -f "$output_path.stdout" && -s "$output_path.stdout" ]]; then
      echo "[ERROR] Output details:" >&2
      cat "$output_path.stdout" >&2
    fi
    rm -f "$output_path" "$output_path.stdout"
  fi
done

rm -f "$TMP_SQL_FILE"

END_TIME_EPOCH=$(date +%s)
END_TIME_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TOTAL_SECONDS=$(( END_TIME_EPOCH - START_TIME_EPOCH ))
DURATION_MINS=$(( (TOTAL_SECONDS + 59) / 60 ))
OVERALL_STATUS="SUCCESS"
if [[ $FAILED_COUNT -gt 0 ]]; then
  OVERALL_STATUS="FAILED"
fi

# 1. Generate BQMS Assessment metadata descriptor (compilerworks-metadata.yaml)
cat > "$OUTPUT_DIR/compilerworks-metadata.yaml" << EOF
format: "snowflake_assessment_tco.zip"
timestamp: $START_TIME_MS
product:
  version: "migration-discovery-scripts:$TOOL_VERSION"
  arguments: "ConnectorArguments{connector=snowflake-mds, assessment=true}"
EOF

append_task_detail "{\"name\":\"compilerworks-metadata.yaml\",\"type\":\"REQUIRED\",\"status\":\"SUCCEEDED\",\"duration_seconds\":0}"

# 2. Generate BQMS Client Telemetry (dumper-telemetry.jsonl)
cat > "$OUTPUT_DIR/dumper-telemetry.jsonl" << EOF
{"id":"$RUN_ID","eventType":"DUMPER_RUN_METRICS","dumperRunStartClientTime":"$START_TIME_ISO","dumperMetadata":{"version":"$TOOL_VERSION","gitCommit":"migration-discovery-scripts","buildDate":"$RUN_DATE"},"payload":[{"id":"snowflake-tco-discovery","measureStartTime":"$START_TIME_ISO","measureEndTime":"$END_TIME_ISO","started_at":"$START_TIME_ISO","completed_at":"$END_TIME_ISO","runDurationInMinutes":$DURATION_MINS,"duration_seconds":$TOTAL_SECONDS,"overallStatus":"$OVERALL_STATUS","status":"$OVERALL_STATUS","failed_tasks":$FAILED_COUNT,"error_summary":"$ERROR_SUMMARY","taskExecutionSummary":[{"count":$SUCCEEDED_COUNT,"status":"SUCCEEDED"},{"count":$FAILED_COUNT,"status":"FAILED"}],"taskDetailedSummary":[$TASK_DETAILS_JSON],"argumentSummary":{"connector":"snowflake-mds","assessment":true,"account_region":"$ACCOUNT_REGION","access_history_available":$ACCESS_HISTORY_AVAILABLE,"extraction_method":"snowflake-cli","date_range_start":"$DATE_RANGE_START","date_range_end":"$DATE_RANGE_END"}}]}
EOF

if [[ $FAILED_COUNT -gt 0 ]]; then
  echo "[ERROR] $FAILED_COUNT extraction task(s) failed. Telemetry written to $OUTPUT_DIR/dumper-telemetry.jsonl. Aborting ZIP creation." >&2
  exit 1
fi

ZIP_OUT_PATH="$OUTPUT_DIR/$ZIP_NAME"
rm -f "$ZIP_OUT_PATH" "./$ZIP_NAME"
(
  cd "$OUTPUT_DIR"
  zip -q "$ZIP_NAME" \
    warehouses_workload_structure_mds.csv \
    warehouse_metering_history_mds.csv \
    table_storage_metrics_mds.csv \
    report_date_range.csv \
    compilerworks-metadata.yaml \
    dumper-telemetry.jsonl
)

cp -f "$ZIP_OUT_PATH" "./$ZIP_NAME" 2>/dev/null || true

echo "Archive: $(pwd)/$ZIP_NAME"
echo "Upload to GCS: gcloud storage cp $ZIP_NAME gs://<your-assessment-bucket>/"
