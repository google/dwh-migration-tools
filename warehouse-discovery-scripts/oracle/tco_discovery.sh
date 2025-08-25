#!/bin/bash

source spinner.sh

ORACLE_CONN="$1"
DURATION_DAYS=7
OUTPUT_DIR="./out"
DISCOVERY_SQLS=(
    "native/used-space-details.sql"     # CDB_SEGMENTS - used space
    "native/osstat.sql"                 # GV$OSSTAT - System statistics like NUM_CPUS
    "native/cell-config.sql"            # GV$CELL -  exadata cell configuration
    "native/db-info.sql"                # V$DATABASE - Database info
    "native/app-schemas-pdbs.sql"       # CDB_PDBS - Pluggable databases info
    "awr/sys-metric-history.sql"        # CDB_HIST_SYSMETRIC_HISTORY - historical system statistics like CPU usage
    "awr/segment-stats.sql"             # CDB_HIST_SEG_STAT - historical segment statistics
)
DISCOVERY_SQL_DIR="$(dirname "$0")/../../dumper/app/src/main/resources/oracle-stats/cdb"
TMP_QUERY_FILE=".query.sql"
EXPORT_SCRIPT="export.sql"
mkdir -p "$OUTPUT_DIR"

# Run each SQL query and export result to CSV file
for sql_file in "${DISCOVERY_SQLS[@]}"; do
  file_path="$DISCOVERY_SQL_DIR/$sql_file"
  base_name=$(basename "$file_path" .sql)
  output_csv="$OUTPUT_DIR/$base_name.csv"

  if [ -f "$file_path" ]; then
    echo "[INFO]: Executing $base_name.sql"

    # Replace JDBC variable placeholder '?' with SQL*Plus substitution 
    sed 's/?/\&1/' "$file_path" > "$TMP_QUERY_FILE"

    # Show spinner
    show_spinner &
    SPINNER_PID=$!

    # Run SQL*Plus
    sqlplus -s "$ORACLE_CONN" "@$EXPORT_SCRIPT" "$TMP_QUERY_FILE" "$output_csv" "$DURATION_DAYS"
    stop_spinner "$SPINNER_PID"
    if [ $? -ne 0 ]; then
      echo "[ERROR]: $base_name extraction failed."
    else
      echo "[SUCCESS]: $base_name.sql extraction ran without errors."
    fi
  else
    echo "[ERROR]: The file '$file_path' does not exist."
  fi
done

# Generate zip metadata files that are required by BigQuery Migration Assessment
cat > $OUTPUT_DIR/compilerworks-metadata.yaml << EOL
format: "oracle_assessment_tco.zip"
timestamp: 1721846085350
product:
  arguments: "ConnectorArguments{connector=oracle-stats, assessment=true}"
EOL

# Build final ZIP artifact that can be used with BigQuery Migration Assessment.
zip -j "oracle_assessment_tco.zip" $OUTPUT_DIR/*.csv $OUTPUT_DIR/*.yaml 
rm -rf "$TMP_QUERY_FILE"