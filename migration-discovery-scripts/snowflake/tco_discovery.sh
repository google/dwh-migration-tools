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
#   ./tco_discovery.sh -a <account> -u <user> [-r <role>] [-w <warehouse>] [-d <lookback_days>] [-o <output_dir>]
#

set -euo pipefail

TOOL_VERSION="1.0.0"
SF_ACCOUNT=""
SF_USER=""
SF_ROLE=""
SF_WAREHOUSE=""
SF_PRIVATE_KEY_PATH=""
LOOKBACK_DAYS=14
OUTPUT_DIR="./out"
ZIP_NAME="snowflake_assessment_tco.zip"
VERBOSE=false

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
  -d, --days              Lookback period in days for historical metrics (default: 14, min: 1, max: 365)
  -o, --output            Output staging directory (default: ./out)
  -v, --verbose           Enable verbose execution output (banners, configuration, timing details)
  -h, --help              Display this help message

Authentication:
  Export SNOWSQL_PWD='<password>' before running, pass -k /path/to/rsa_key.p8,
  or configure credentials in ~/.snowsql/config.

Examples:
  # Standard 14-day discovery
  export SNOWSQL_PWD='<your_snowflake_password>'
  $0 -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH

  # 365-day annual lookback for seasonal TCO modeling
  $0 -a myorg-myaccount -u myuser -r ACCOUNTADMIN -w COMPUTE_WH -d 365
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
    -d|--days)             LOOKBACK_DAYS="$2"; shift 2 ;;
    -o|--output)           OUTPUT_DIR="$2"; shift 2 ;;
    -v|--verbose)          VERBOSE=true; shift ;;
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

if ! [[ "$LOOKBACK_DAYS" =~ ^[0-9]+$ ]] || [[ "$LOOKBACK_DAYS" -lt 1 ]] || [[ "$LOOKBACK_DAYS" -gt 365 ]]; then
  echo "[ERROR] Lookback days (-d) must be an integer between 1 and 365." >&2
  exit 1
fi

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
  START_TIME_MS=$(python3 -c 'import time; print(int(time.time() * 1000))' 2>/dev/null || echo "${START_TIME_EPOCH}000")
fi
START_TIME_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
RUN_DATE=$(date -u +"%Y-%m-%d")
RUN_ID="snowflake-discovery-$(date +%Y%m%d%H%M%S)"

if [[ "$VERBOSE" == "true" ]]; then
  echo "================================================================="
  echo "  Snowflake Discovery & TCO Extraction for BigQuery Migration"
  echo "================================================================="
  echo "Account:          $SF_ACCOUNT"
  echo "User:             $SF_USER"
  echo "Role:             ${SF_ROLE:-<user-default>}"
  echo "Warehouse:        ${SF_WAREHOUSE:-<active-warehouse>}"
  echo "Lookback Days:    $LOOKBACK_DAYS"
  echo "Output Directory: $OUTPUT_DIR"
  echo "Run ID:           $RUN_ID"
  echo "-----------------------------------------------------------------"
fi

# Notify if neither SNOWSQL_PWD nor -k is set
if [[ -z "${SNOWSQL_PWD:-}" && -z "$SF_PRIVATE_KEY_PATH" ]]; then
  if [[ "$VERBOSE" == "true" ]]; then
    echo "[INFO] Neither SNOWSQL_PWD nor -k (--private-key-path) is set; relying on ~/.snowsql/config."
  fi
fi

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
if [[ "$VERBOSE" == "true" ]]; then
  echo -n "[INFO] Testing Snowflake connection... "
fi
CONN_TEST_QUERY="${SESSION_INIT} SELECT CURRENT_REGION();"
if CONN_OUTPUT=$("${SNOWSQL_CMD[@]}" -q "$CONN_TEST_QUERY" -o output_format=csv -o header=false -o timing=false -o friendly=false -o quiet=false < /dev/null 2>&1); then
  ACCOUNT_REGION=$(echo "$CONN_OUTPUT" | tr -d '\r"' | awk 'NF {last=$0} END {print last}')
  ACCOUNT_REGION="${ACCOUNT_REGION:-UNKNOWN}"
  if [[ "$VERBOSE" == "true" ]]; then
    echo "OK (region: $ACCOUNT_REGION)"
  elif [[ "$ACCOUNT_REGION" == "UNKNOWN" ]]; then
    echo "[WARN] Account region is UNKNOWN"
  fi
else
  if [[ "$VERBOSE" == "true" ]]; then
    echo "FAILED"
  fi
  echo "[ERROR] Could not authenticate or connect to Snowflake." >&2
  echo "[HINT] Ensure SNOWSQL_PWD is exported (export SNOWSQL_PWD='<password>'), -k <private_key_path> is passed, or credentials are set in ~/.snowsql/config." >&2
  echo "$CONN_OUTPUT" >&2
  exit 1
fi

# Pre-flight check on ACCESS_HISTORY
ACCESS_HISTORY_AVAILABLE=true
if [[ "$VERBOSE" == "true" ]]; then
  echo -n "[INFO] Checking access to SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY... "
fi
CHECK_QUERY="${SESSION_INIT} SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1;"
if "${SNOWSQL_CMD[@]}" -q "$CHECK_QUERY" -o output_format=plain -o header=false -o timing=false -o friendly=false -o quiet=true > /dev/null 2>&1; then
  if [[ "$VERBOSE" == "true" ]]; then
    echo "OK (ACCESS_HISTORY available)"
  fi
else
  if [[ "$VERBOSE" == "true" ]]; then
    echo "UNAVAILABLE"
  fi
  echo "[WARN] SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY is inaccessible (e.g. Snowflake Standard Edition or missing GOVERNANCE_VIEWER role)."
  echo "[WARN] Falling back to IsMultiObject = FALSE in warehouses_workload_structure_mds.sql."
  ACCESS_HISTORY_AVAILABLE=false
fi

# Extraction tasks: "sql_file:output_csv_name:header_pattern:description"
TASKS=(
  "warehouses_workload_structure_mds.sql:warehouses_workload_structure_mds.csv:^\"?WarehouseName\"?,\"?AggregateTime\"?:Virtual Warehouse Workload Structure"
  "warehouse_metering_history_mds.sql:warehouse_metering_history_mds.csv:^\"?StartTime\"?:Virtual Warehouse Metering History"
  "table_storage_metrics_mds.sql:table_storage_metrics_mds.csv:^\"?TableName\"?,\"?ActiveBytes\"?:Table Storage Breakdown"
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

  if [[ "$VERBOSE" == "true" ]]; then
    echo -n "[INFO] Extracting $desc ($output_csv)... "
  fi

  if [[ ! -f "$input_path" ]]; then
    if [[ "$VERBOSE" == "true" ]]; then
      echo "FAILED"
    else
      echo "$output_csv  FAILED: SQL template '$input_path' does not exist"
    fi
    echo "[ERROR] SQL template '$input_path' does not exist." >&2
    FAILED_COUNT=$((FAILED_COUNT + 1))
    append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"FAILED\",\"duration_seconds\":0,\"error\":\"missing SQL template\"}"
    append_error_summary "$output_csv: missing SQL template"
    continue
  fi

  # Render SQL: strip SET LOOKBACK_DAYS (portable across GNU and BSD/macOS sed) and substitute variables
  rendered_sql=$(sed -E \
    -e "s/^[[:space:]]*[Ss][Ee][Tt][[:space:]]+[Ll][Oo][Oo][Kk][Bb][Aa][Cc][Kk]_[Dd][Aa][Yy][Ss][[:space:]]*=[[:space:]]*[0-9]+;[[:space:]]*$//" \
    -e "s/\\\$LOOKBACK_DAYS/${LOOKBACK_DAYS}/g" \
    -e "s/&1/${LOOKBACK_DAYS}/g" \
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
        if [[ "$VERBOSE" == "true" ]]; then
          echo "DONE ($row_count rows, ${query_elapsed_secs}s)"
        else
          echo "$output_csv  $row_count rows  ${query_elapsed_secs}s"
        fi
      else
        row_count=0
        if [[ "$VERBOSE" == "true" ]]; then
          echo "DONE (Empty / 0 rows, ${query_elapsed_secs}s)"
        else
          echo "$output_csv  0 rows  ${query_elapsed_secs}s"
        fi
      fi
      SUCCEEDED_COUNT=$((SUCCEEDED_COUNT + 1))
      append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"SUCCEEDED\",\"duration_seconds\":$query_elapsed_secs,\"row_count\":$row_count}"
    else
      FAILED_COUNT=$((FAILED_COUNT + 1))
      append_task_detail "{\"name\":\"$output_csv\",\"type\":\"REQUIRED\",\"status\":\"FAILED\",\"duration_seconds\":$query_elapsed_secs,\"error\":\"missing expected CSV header\"}"
      append_error_summary "$output_csv: missing expected CSV header"
      if [[ "$VERBOSE" == "true" ]]; then
        echo "FAILED (${query_elapsed_secs}s)"
      else
        echo "$output_csv  FAILED (${query_elapsed_secs}s): missing expected CSV header"
      fi
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
    if [[ "$VERBOSE" == "true" ]]; then
      echo "FAILED (${query_elapsed_secs}s)"
    else
      echo "$output_csv  FAILED (${query_elapsed_secs}s): snowsql execution failure"
    fi
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

if [[ "$VERBOSE" == "true" ]]; then
  echo "-----------------------------------------------------------------"
  echo "[INFO] Creating metadata descriptor & telemetry..."
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
{"id":"$RUN_ID","eventType":"DUMPER_RUN_METRICS","dumperRunStartClientTime":"$START_TIME_ISO","dumperMetadata":{"version":"$TOOL_VERSION","gitCommit":"migration-discovery-scripts","buildDate":"$RUN_DATE"},"payload":[{"id":"snowflake-tco-discovery","measureStartTime":"$START_TIME_ISO","measureEndTime":"$END_TIME_ISO","started_at":"$START_TIME_ISO","completed_at":"$END_TIME_ISO","runDurationInMinutes":$DURATION_MINS,"duration_seconds":$TOTAL_SECONDS,"overallStatus":"$OVERALL_STATUS","status":"$OVERALL_STATUS","failed_tasks":$FAILED_COUNT,"error_summary":"$ERROR_SUMMARY","taskExecutionSummary":[{"count":$SUCCEEDED_COUNT,"status":"SUCCEEDED"},{"count":$FAILED_COUNT,"status":"FAILED"}],"taskDetailedSummary":[$TASK_DETAILS_JSON],"argumentSummary":{"connector":"snowflake-mds","assessment":true,"account_region":"$ACCOUNT_REGION","lookback_days":$LOOKBACK_DAYS,"access_history_available":$ACCESS_HISTORY_AVAILABLE,"extraction_method":"snowflake-cli"}}]}
EOF

if [[ $FAILED_COUNT -gt 0 ]]; then
  echo "[ERROR] $FAILED_COUNT extraction task(s) failed. Telemetry written to $OUTPUT_DIR/dumper-telemetry.jsonl. Aborting ZIP creation." >&2
  exit 1
fi

if [[ "$VERBOSE" == "true" ]]; then
  echo "[INFO] Building final ZIP package: $ZIP_NAME..."
fi
ZIP_OUT_PATH="$OUTPUT_DIR/$ZIP_NAME"
rm -f "$ZIP_OUT_PATH" "./$ZIP_NAME"
(
  cd "$OUTPUT_DIR"
  zip -q "$ZIP_NAME" \
    warehouses_workload_structure_mds.csv \
    warehouse_metering_history_mds.csv \
    table_storage_metrics_mds.csv \
    compilerworks-metadata.yaml \
    dumper-telemetry.jsonl
)

cp -f "$ZIP_OUT_PATH" "./$ZIP_NAME" 2>/dev/null || true

FINAL_END_EPOCH=$(date +%s)
TOTAL_ELAPSED_SECS=$(( FINAL_END_EPOCH - START_TIME_EPOCH ))
ELAPSED_MINS=$(( TOTAL_ELAPSED_SECS / 60 ))
ELAPSED_REM_SECS=$(( TOTAL_ELAPSED_SECS % 60 ))

if [[ "$VERBOSE" == "true" ]]; then
  echo "================================================================="
  echo "  EXTRACTION COMPLETE"
  echo "================================================================="
  echo "Archive created:      $(pwd)/$ZIP_NAME (and $ZIP_OUT_PATH)"
  echo "Total execution time: ${ELAPSED_MINS}m ${ELAPSED_REM_SECS}s (${TOTAL_ELAPSED_SECS}s)"
  echo ""
  echo "Next steps:"
  echo "1. Upload '$ZIP_NAME' to your Google Cloud Storage bucket:"
  echo "     gcloud storage cp $ZIP_NAME gs://<your-assessment-bucket>/"
  echo "2. Create a new Migration Assessment in the Google Cloud Console"
  echo "   pointing to the bucket location."
  echo "================================================================="
else
  echo "Archive: $(pwd)/$ZIP_NAME"
  echo "Upload to GCS: gcloud storage cp $ZIP_NAME gs://<your-assessment-bucket>/"
fi
