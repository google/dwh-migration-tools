/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifierIfNeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteQualifiedName;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.constructWarnings;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.printCheckConstraints;
import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ERROR_TYPE;

import com.databricks.sdk.core.DatabricksException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MultiFileMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ViewsFormat;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory, SQL statement runner, and row writers for the {@code hive_metastore} metadata extraction
 * tasks in their archive order. One task writes the tables, columns, views and table constraints
 * files: it reads each table with {@code DESCRIBE TABLE EXTENDED ... AS JSON}, and the partition
 * columns and size of Delta tables with {@code DESCRIBE DETAIL}, and writes its rows right away.
 */
public final class HiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(HiveMetastoreTasks.class);

  // A record is an Object[] indexed by Header.ordinal(), which is the column order that
  // FORMAT.withHeader(Header.class) writes, so each value is printed under its column.
  private static final int TABLES_RECORD_SIZE = TablesFormat.Header.values().length;
  private static final int COLUMNS_RECORD_SIZE = ColumnsFormat.Header.values().length;

  private static final String PARTITION_COLUMNS_DETAIL = "partitionColumns";
  private static final String SIZE_IN_BYTES_DETAIL = "sizeInBytes";
  private static final String DEFAULT_COLLATION = "UTF8_BINARY";
  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);

  private final Object schemasLock = new Object();

  @GuardedBy("schemasLock")
  private ImmutableList<String> schemas;

  HiveMetastoreTasks() {}

  public static ImmutableList<Task<?>> create() {
    HiveMetastoreTasks tasks = new HiveMetastoreTasks();
    return ImmutableList.of(
        new MetadataTask(
            CatalogsFormat.HMS_ZIP_ENTRY_NAME,
            CatalogsFormat.Header.class,
            "containing the hive_metastore catalog",
            tasks::writeCatalogs),
        new MetadataTask(
            SchemataFormat.HMS_ZIP_ENTRY_NAME,
            SchemataFormat.Header.class,
            "from hive_metastore SHOW SCHEMAS",
            tasks::writeSchemata),
        new MultiFileMetadataTask(
            ImmutableMap.of(
                TablesFormat.HMS_ZIP_ENTRY_NAME, TablesFormat.Header.class,
                ColumnsFormat.HMS_ZIP_ENTRY_NAME, ColumnsFormat.Header.class,
                ViewsFormat.HMS_ZIP_ENTRY_NAME, ViewsFormat.Header.class,
                TableConstraintsFormat.HMS_ZIP_ENTRY_NAME, TableConstraintsFormat.Header.class),
            "from hive_metastore SHOW TABLES and DESCRIBE TABLE EXTENDED",
            tasks::writeTableFiles));
  }

  /**
   * Writes the hive_metastore catalog row without a SQL statement, so {@code
   * DatabricksMetadataConnector} leaves this task out of the all-failed check.
   */
  ImmutableList<String> writeCatalogs(DatabricksHandle handle, CSVPrinter printer)
      throws IOException {
    printer.printRecord(HIVE_METASTORE_CATALOG, null);
    return ImmutableList.of();
  }

  ImmutableList<String> writeSchemata(DatabricksHandle handle, CSVPrinter printer)
      throws IOException {
    for (String schema : listSchemas(handle)) {
      printer.printRecord(HIVE_METASTORE_CATALOG, schema);
    }

    return ImmutableList.of();
  }

  /**
   * Writes the rows of {@code tables-hms.csv}, {@code columns-hms.csv}, {@code views-hms.csv} and
   * {@code table_constraints-hms.csv}: the tables of each schema are listed with {@code SHOW
   * TABLES}, then each table is described and its rows are written. A table whose {@code DESCRIBE
   * TABLE} fails gets a tables row with its names and the {@code TableType} ERROR.
   */
  ImmutableList<String> writeTableFiles(DatabricksHandle handle, Map<String, CSVPrinter> printers)
      throws IOException {
    CSVPrinter tables = printers.get(TablesFormat.HMS_ZIP_ENTRY_NAME);
    CSVPrinter columns = printers.get(ColumnsFormat.HMS_ZIP_ENTRY_NAME);
    CSVPrinter views = printers.get(ViewsFormat.HMS_ZIP_ENTRY_NAME);
    CSVPrinter constraints = printers.get(TableConstraintsFormat.HMS_ZIP_ENTRY_NAME);
    ImmutableList.Builder<TableName> listedTables = ImmutableList.builder();
    String showWarning =
        handle.runForEach(
            "SHOW TABLES",
            listSchemas(handle),
            schema -> showTables(handle, schema),
            (schema, schemaTables) -> listedTables.addAll(schemaTables));
    String describeWarning =
        handle.runForEach(
            "DESCRIBE TABLE EXTENDED AS JSON",
            listedTables.build(),
            table -> describeTable(handle, table),
            (table, description) ->
                printTable(handle, table, description, tables, columns, views, constraints),
            (table, failure) -> tables.printRecord(tableErrorRecord(table)));
    return constructWarnings(showWarning, describeWarning);
  }

  private ImmutableList<String> listSchemas(DatabricksHandle handle) throws IOException {
    synchronized (this.schemasLock) {
      if (this.schemas == null) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        StatementResult rows =
            handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SHOW SCHEMAS");
        for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
          if (handle.filter().acceptsSchema(row.get(0))) {
            builder.add(row.get(0));
          }
        }

        this.schemas = builder.build();
      }

      return this.schemas;
    }
  }

  private static ImmutableList<TableName> showTables(DatabricksHandle handle, String schema)
      throws IOException {
    String query = "SHOW TABLES IN " + quoteIdentifier(schema);
    ImmutableList.Builder<TableName> builder = ImmutableList.builder();
    StatementResult rows = handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query);
    // The cells of a row are the schema, the table name and isTemporary.
    for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
      if (!"true".equalsIgnoreCase(row.get(2))) {
        builder.add(new TableName(schema, row.get(1)));
      }
    }
    return builder.build();
  }

  private static JsonNode describeTable(DatabricksHandle handle, TableName table)
      throws IOException {
    String query = "DESCRIBE TABLE EXTENDED " + table.quotedName() + " AS JSON";
    StatementResult rows = handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query);
    String json = cell(rows.nextRow(), 0);
    JsonNode description = json == null ? null : JSON_READER.readTree(json);
    if (description == null || !description.isObject()) {
      throw new IOException(
          "DESCRIBE TABLE AS JSON returned no JSON object for " + table.quotedName() + ".");
    }
    return description;
  }

  /**
   * Prints the rows of a table from its {@code DESCRIBE TABLE} output. For a Delta table, {@code
   * DESCRIBE TABLE EXTENDED ... AS JSON} omits the partition columns and the live table size, so
   * they are read with {@code DESCRIBE DETAIL}. The CHECK constraints of a Delta table are in its
   * {@code table_properties}.
   */
  private static void printTable(
      DatabricksHandle handle,
      TableName table,
      JsonNode description,
      CSVPrinter tables,
      CSVPrinter columns,
      CSVPrinter views,
      CSVPrinter constraints)
      throws IOException {
    JsonNode partitionColumns = description.path("partition_columns");
    String sizeInBytes = description.path("statistics").path("size_in_bytes").asText("");
    if ("delta".equalsIgnoreCase(description.path("provider").asText(""))) {
      String detailQuery = "DESCRIBE DETAIL " + table.quotedName();
      try {
        StatementResult detail =
            handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, detailQuery);
        List<String> row = detail.nextRow();
        String detailSize = cell(row, detail.columnNames().indexOf(SIZE_IN_BYTES_DETAIL));
        if (detailSize != null) {
          sizeInBytes = detailSize;
        }
        String detailPartitions = cell(row, detail.columnNames().indexOf(PARTITION_COLUMNS_DETAIL));
        if (detailPartitions != null) {
          partitionColumns = JSON_READER.readTree(detailPartitions);
        } else {
          logger.warn(
              "Writing the columns of {} without partition indexes because DESCRIBE DETAIL failed:"
                  + " DESCRIBE DETAIL returned no partitionColumns.",
              table.quotedName());
        }
      } catch (IOException | DatabricksException e) {
        logger.warn(
            "Writing {} without partition indexes and size because DESCRIBE DETAIL failed: {}",
            table.quotedName(),
            e.getMessage());
      }
    }

    String tableType = description.path("type").asText("");
    // A hive_metastore table has no SecurableKind or PipelineId.
    tables.printRecord(
        HIVE_METASTORE_CATALOG,
        table.schema,
        table.name,
        tableType,
        description.path("provider").asText("").toUpperCase(Locale.ROOT),
        description.path("location").asText(""),
        null,
        null,
        sizeInBytes);
    int ordinalPosition = 0;
    for (JsonNode column : description.path("columns")) {
      columns.printRecord(columnRecord(table, ordinalPosition++, column, partitionColumns));
    }

    if ("VIEW".equalsIgnoreCase(tableType)) {
      views.printRecord(
          HIVE_METASTORE_CATALOG,
          table.schema,
          table.name,
          description.path("view_text").asText(""));
    }

    printCheckConstraints(
        constraints,
        HIVE_METASTORE_CATALOG,
        table.schema,
        table.name,
        description.path("table_properties"));
  }

  /** Returns the record of {@code columns-hms.csv} for a column of a {@code DESCRIBE TABLE} row. */
  private static Object[] columnRecord(
      TableName table, int ordinalPosition, JsonNode column, JsonNode partitionColumns) {
    JsonNode type = column.path("type");
    String columnName = column.path("name").asText("");
    Object[] record = new Object[COLUMNS_RECORD_SIZE];
    record[ColumnsFormat.Header.TableCatalog.ordinal()] = HIVE_METASTORE_CATALOG;
    record[ColumnsFormat.Header.TableSchema.ordinal()] = table.schema;
    record[ColumnsFormat.Header.TableName.ordinal()] = table.name;
    record[ColumnsFormat.Header.OrdinalPosition.ordinal()] = ordinalPosition;
    record[ColumnsFormat.Header.ColumnName.ordinal()] = columnName;
    record[ColumnsFormat.Header.DataType.ordinal()] =
        type.path("name").asText("").toUpperCase(Locale.ROOT);
    record[ColumnsFormat.Header.FullDataType.ordinal()] = formatDataType(type);
    record[ColumnsFormat.Header.IsNullable.ordinal()] =
        column.path("nullable").asBoolean(true) ? "YES" : "NO";
    record[ColumnsFormat.Header.ColumnDefault.ordinal()] = column.path("default").asText("");
    // Only the char and varchar types have a length, and only decimal a precision and a scale.
    record[ColumnsFormat.Header.CharacterMaximumLength.ordinal()] = type.path("length").asText("");
    record[ColumnsFormat.Header.NumericPrecision.ordinal()] = type.path("precision").asText("");
    record[ColumnsFormat.Header.NumericScale.ordinal()] = type.path("scale").asText("");
    record[ColumnsFormat.Header.Comment.ordinal()] = column.path("comment").asText("");
    // The column of the DESCRIBE TABLE ... AS JSON output as it is, such as
    // {"name":"id","type":{"name":"bigint"},"nullable":false}.
    record[ColumnsFormat.Header.TypeJson.ordinal()] = column.toString();
    for (int index = 0; index < partitionColumns.size(); index++) {
      if (partitionColumns.get(index).asText().equalsIgnoreCase(columnName)) {
        record[ColumnsFormat.Header.PartitionIndex.ordinal()] = index;
        break;
      }
    }
    return record;
  }

  /** Returns the row of a table whose {@code DESCRIBE TABLE} failed: its names and ERROR. */
  private static Object[] tableErrorRecord(TableName table) {
    Object[] record = new Object[TABLES_RECORD_SIZE];
    record[TablesFormat.Header.TableCatalog.ordinal()] = HIVE_METASTORE_CATALOG;
    record[TablesFormat.Header.TableSchema.ordinal()] = table.schema;
    record[TablesFormat.Header.TableName.ordinal()] = table.name;
    record[TablesFormat.Header.TableType.ordinal()] = ERROR_TYPE;
    return record;
  }

  /** Returns a {@code DESCRIBE TABLE ... AS JSON} column type as Databricks prints it. */
  private static String formatDataType(JsonNode type) {
    String name = type.path("name").asText("").toLowerCase(Locale.ROOT);
    switch (name) {
      case "decimal":
        return "decimal("
            + type.path("precision").asText()
            + ","
            + type.path("scale").asText()
            + ")";
      case "char":
      case "varchar":
        return name + "(" + type.path("length").asText() + ")";
      case "string":
        String collation = type.path("collation").asText("");
        return collation.isEmpty() || DEFAULT_COLLATION.equalsIgnoreCase(collation)
            ? "string"
            : "string collate " + collation;
      case "interval":
        String startUnit = type.path("start_unit").asText();
        String endUnit = type.path("end_unit").asText();
        return startUnit.equals(endUnit)
            ? "interval " + startUnit
            : "interval " + startUnit + " to " + endUnit;
      case "array":
        return "array<" + formatDataType(type.path("element_type")) + ">";
      case "map":
        return "map<"
            + formatDataType(type.path("key_type"))
            + ","
            + formatDataType(type.path("value_type"))
            + ">";
      case "struct":
        StringBuilder struct = new StringBuilder("struct<");
        for (JsonNode field : type.path("fields")) {
          if (struct.length() > "struct<".length()) {
            struct.append(',');
          }
          struct.append(formatStructField(field));
        }
        return struct.append('>').toString();
      case "timestamp_ltz":
        // The JSON type name of TIMESTAMP.
        return "timestamp";
      default:
        return name;
    }
  }

  private static String formatStructField(JsonNode field) {
    StringBuilder result =
        new StringBuilder(quoteIdentifierIfNeeded(field.path("name").asText("")))
            .append(':')
            .append(formatDataType(field.path("type")));
    if (!field.path("nullable").asBoolean(true)) {
      result.append(" not null");
    }
    String comment = field.path("comment").asText("");
    if (!comment.isEmpty()) {
      result.append(" comment ").append(quoteStringLiteral(comment));
    }
    return result.toString();
  }

  /** The schema and name of a table listed by {@code SHOW TABLES}. */
  static final class TableName {
    final String schema;
    final String name;

    TableName(String schema, String name) {
      this.schema = schema;
      this.name = name;
    }

    String quotedName() {
      return quoteQualifiedName(this.schema, this.name);
    }

    /** Returns the quoted name, such as {@code `sales`.`orders`}. */
    @Override
    public String toString() {
      return quotedName();
    }
  }
}
