#!/bin/bash

ORACLE_CONN="$1"
DURATION_DAYS=30
OUTPUT_DIR="./out"
DISCOVERY_SQLS=(
    "awr/sys-metric-history.sql"        # CDB_HIST_SYSMETRIC_HISTORY - historical system statistics like CPU usage
    "awr/segment-stats.sql"             # CDB_HIST_SEG_STAT - historical segment statistics
    "native/used-space-details.sql"     # CDB_SEGMENTS - used space
    "native/osstat.sql"                 # GV$OSSTAT - System statistics like NUM_CPUS
    "native/cell-config.sql"            # GV$CELL -  exadata cell configuration
    "native/db-info.sql"                # V$DATABASE - Database info
    "native/app-schemas-pdbs.sql"       # CDB_PDBS - Pluggable databases info
)
DISCOVERY_SQL_DIR="$(dirname "$0")/../../dumper/app/src/main/resources/oracle-stats/cdb"

# Run each SQL query and export result to CSV file
for sql_file in "${DISCOVERY_SQLS[@]}"; do
    file_path="$DISCOVERY_SQL_DIR/$sql_file"
    base_name=$(basename "$file_path" .sql)
    output_csv="$OUTPUT_DIR/$base_name.csv"
    echo "Executing $base_name.sql"
    sqlplus -s "$ORACLE_CONN" @export.sql "$file_path" "$output_csv" "$DURATION_DAYS"
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
