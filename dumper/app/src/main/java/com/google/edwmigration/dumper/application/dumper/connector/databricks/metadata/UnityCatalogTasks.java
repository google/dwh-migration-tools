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

import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteQualifiedName;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.constructWarnings;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.printCheckConstraints;
import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ERROR_TYPE;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnMask;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListSchemasRequest;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableRowFilter;
import com.databricks.sdk.service.catalog.TableType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MultiFileMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnMasksFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.RowFiltersFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ViewsFormat;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory, Unity Catalog REST API readers, and CSV row writers for the Unity Catalog metadata
 * extraction tasks in their archive order. The schemas are listed once per run. One task writes the
 * tables, columns, views, column masks and row filters files from one {@code tables.list} call per
 * schema, so the rows are written schema by schema. Every REST call sets {@code
 * include_browse=true}, so objects with only the BROWSE privilege are returned too, and throws the
 * {@code DatabricksException} of the SDK when the API returns an error.
 */
public final class UnityCatalogTasks {

  private static final Pattern DECIMAL_TYPE_PATTERN =
      Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CHAR_TYPE_PATTERN =
      Pattern.compile("(?:var)?char\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Logger logger = LoggerFactory.getLogger(UnityCatalogTasks.class);

  // A record is an Object[] indexed by Header.ordinal(), which is the column order that
  // FORMAT.withHeader(Header.class) writes, so each value is printed under its column.
  private static final int COLUMNS_RECORD_SIZE = ColumnsFormat.Header.values().length;
  private static final int TABLE_CONSTRAINTS_RECORD_SIZE =
      TableConstraintsFormat.Header.values().length;

  // 0 lets the server choose the page size; the SDK and listTables follow next_page_token.
  private static final long SERVER_PAGE_SIZE = 0L;
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables";
  private static final String TOTAL_SIZE_PROPERTY = "spark.sql.statistics.totalSize";
  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);
  private static final ObjectWriter JSON_WRITER = new ObjectMapper().writer();
  // Reads the SDK models from the JSON responses with the settings of the SDK.
  private static final ObjectMapper SDK_MAPPER = SerDeUtils.createMapper();
  private static final Splitter PARENT_TABLE_SPLITTER = Splitter.on('.').limit(3);

  private final Object schemasLock = new Object();

  /** The accepted (catalog, schema) pairs, listed by the first task that needs them. */
  @GuardedBy("schemasLock")
  private ImmutableList<SchemaName> schemas;

  /**
   * The warning of the {@code schemas.list} calls, or null, set with {@code schemas} and shown in
   * the summary of the schemata task.
   */
  @GuardedBy("schemasLock")
  private String schemasWarning;

  UnityCatalogTasks() {}

  public static ImmutableList<Task<?>> create() {
    UnityCatalogTasks tasks = new UnityCatalogTasks();
    return ImmutableList.of(
        new MetadataTask(
            CatalogsFormat.ZIP_ENTRY_NAME,
            CatalogsFormat.Header.class,
            "from Unity Catalog catalogs.list",
            tasks::writeCatalogs),
        new MetadataTask(
            SchemataFormat.ZIP_ENTRY_NAME,
            SchemataFormat.Header.class,
            "from Unity Catalog schemas.list",
            tasks::writeSchemata),
        new MultiFileMetadataTask(
            ImmutableMap.of(
                TablesFormat.ZIP_ENTRY_NAME, TablesFormat.Header.class,
                ColumnsFormat.ZIP_ENTRY_NAME, ColumnsFormat.Header.class,
                ViewsFormat.ZIP_ENTRY_NAME, ViewsFormat.Header.class,
                ColumnMasksFormat.ZIP_ENTRY_NAME, ColumnMasksFormat.Header.class,
                RowFiltersFormat.ZIP_ENTRY_NAME, RowFiltersFormat.Header.class),
            "from Unity Catalog tables.list",
            tasks::writeTableFiles),
        new MetadataTask(
            TableConstraintsFormat.ZIP_ENTRY_NAME,
            TableConstraintsFormat.Header.class,
            "from Unity Catalog tables.get",
            tasks::writeTableConstraints));
  }

  ImmutableList<String> writeCatalogs(DatabricksHandle handle, CSVPrinter printer)
      throws IOException {
    for (CatalogInfo catalog : listAcceptedCatalogs(handle)) {
      printer.printRecord(catalog.getName(), catalog.getOwner());
    }

    return ImmutableList.of();
  }

  ImmutableList<String> writeSchemata(DatabricksHandle handle, CSVPrinter printer)
      throws IOException {
    for (SchemaName schema : listSchemas(handle)) {
      printer.printRecord(schema.catalog, schema.name);
    }

    synchronized (this.schemasLock) {
      return constructWarnings(this.schemasWarning);
    }
  }

  /**
   * Writes the rows of {@code tables.csv}, {@code columns.csv}, {@code views.csv}, {@code
   * column_masks.csv} and {@code row_filters.csv} from one {@code tables.list} call per schema. The
   * task summary warns about the tables with only the BROWSE privilege, because the REST API leaves
   * out their storage locations, pipeline IDs, sizes, view definitions and row filters.
   */
  ImmutableList<String> writeTableFiles(DatabricksHandle handle, Map<String, CSVPrinter> printers)
      throws IOException {
    CSVPrinter tables = printers.get(TablesFormat.ZIP_ENTRY_NAME);
    CSVPrinter columns = printers.get(ColumnsFormat.ZIP_ENTRY_NAME);
    CSVPrinter views = printers.get(ViewsFormat.ZIP_ENTRY_NAME);
    CSVPrinter columnMasks = printers.get(ColumnMasksFormat.ZIP_ENTRY_NAME);
    CSVPrinter rowFilters = printers.get(RowFiltersFormat.ZIP_ENTRY_NAME);
    // The catalog names of the browse-only tables, and the number of all tables.
    Multiset<String> browseOnlyTables = TreeMultiset.create();
    AtomicInteger tableCount = new AtomicInteger();
    String listWarning =
        handle.runForEach(
            "tables.list",
            listSchemas(handle),
            schema -> listTables(handle, schema.catalog, schema.name),
            (schema, schemaTables) -> {
              for (ListedTable table : schemaTables) {
                printTable(table, tables, columns, views, columnMasks, rowFilters);
                if (Boolean.TRUE.equals(table.info.getBrowseOnly())) {
                  browseOnlyTables.add(schema.catalog);
                }
              }
              tableCount.addAndGet(schemaTables.size());
            });
    String browseOnlyWarning =
        browseOnlyWarning(
            browseOnlyTables,
            tableCount.get(),
            "tables",
            "storage locations, pipeline IDs, sizes, view definitions and row filters",
            "SELECT");
    return constructWarnings(listWarning, browseOnlyWarning);
  }

  /**
   * Writes the key and CHECK constraints of the tables that aren't views, such as VIEW or
   * MATERIALIZED_VIEW. {@code tables.list} doesn't return constraints, so the names of the tables
   * are listed first, without their columns and properties, then {@code tables.get} is called for
   * each table. A table whose {@code tables.get} call fails gets a row with its names and the
   * {@code ConstraintType} ERROR.
   */
  ImmutableList<String> writeTableConstraints(DatabricksHandle handle, CSVPrinter printer)
      throws IOException {
    ImmutableList.Builder<TableName> tables = ImmutableList.builder();
    String listWarning =
        handle.runForEach(
            "tables.list",
            listSchemas(handle),
            schema -> listTableNamesAndTypes(handle, schema.catalog, schema.name),
            (schema, schemaTables) -> {
              for (TableInfo table : schemaTables) {
                TableType tableType = table.getTableType();
                if (tableType == null || !tableType.name().contains("VIEW")) {
                  tables.add(
                      new TableName(
                          table.getCatalogName(), table.getSchemaName(), table.getName()));
                }
              }
            });
    String getWarning =
        handle.runForEach(
            "tables.get",
            tables.build(),
            table -> getTableJson(handle, table),
            (table, tableJson) -> printConstraints(printer, table, tableJson),
            (table, failure) -> printer.printRecord(constraintErrorRecord(table)));
    return constructWarnings(listWarning, getWarning);
  }

  /**
   * Returns the (catalog, schema) pairs accepted by the filter: the schemas of {@code schemas.list}
   * for each catalog of {@code catalogs.list}.
   */
  private ImmutableList<SchemaName> listSchemas(DatabricksHandle handle) throws IOException {
    synchronized (this.schemasLock) {
      if (this.schemas == null) {
        ImmutableList.Builder<String> catalogs = ImmutableList.builder();
        for (CatalogInfo catalog : listAcceptedCatalogs(handle)) {
          catalogs.add(catalog.getName());
        }

        ImmutableList.Builder<SchemaName> builder = ImmutableList.builder();
        this.schemasWarning =
            handle.runForEach(
                "schemas.list",
                catalogs.build(),
                catalog -> listCatalogSchemas(handle, catalog),
                (catalog, catalogSchemas) -> {
                  for (SchemaInfo schema : catalogSchemas) {
                    if (handle.filter().acceptsSchema(schema.getName())) {
                      builder.add(new SchemaName(catalog, schema.getName()));
                    }
                  }
                });
        this.schemas = builder.build();
      }

      return this.schemas;
    }
  }

  /** Returns the Unity Catalog catalogs of {@code catalogs.list} accepted by the filter. */
  private static ImmutableList<CatalogInfo> listAcceptedCatalogs(DatabricksHandle handle) {
    DatabricksFilter filter = handle.filter();
    ImmutableList.Builder<CatalogInfo> catalogs = ImmutableList.builder();
    for (CatalogInfo catalog :
        handle
            .client()
            .catalogs()
            .list(
                new ListCatalogsRequest().setIncludeBrowse(true).setMaxResults(SERVER_PAGE_SIZE))) {
      String name = catalog.getName();
      if (!HIVE_METASTORE_CATALOG.equalsIgnoreCase(name) && filter.acceptsCatalog(name)) {
        catalogs.add(catalog);
      }
    }
    return catalogs.build();
  }

  private static ImmutableList<SchemaInfo> listCatalogSchemas(
      DatabricksHandle handle, String catalogName) {
    return ImmutableList.copyOf(
        handle
            .client()
            .schemas()
            .list(
                new ListSchemasRequest()
                    .setCatalogName(catalogName)
                    .setIncludeBrowse(true)
                    .setMaxResults(SERVER_PAGE_SIZE)));
  }

  /**
   * Lists the tables of a schema with their columns. The request is the one of {@code
   * TablesAPI.list}, but the response is read as JSON, because the SDK {@link TableInfo} has no
   * {@code securable_kind}. Of the properties, only {@code spark.sql.statistics.totalSize} is kept,
   * because a table can have thousands of statistics properties and the tables of a schema stay in
   * memory until they are written.
   */
  private static ImmutableList<ListedTable> listTables(
      DatabricksHandle handle, String catalogName, String schemaName) throws IOException {
    ImmutableList.Builder<ListedTable> tables = ImmutableList.builder();
    String pageToken = "";
    do {
      Request request =
          new Request(Request.GET, TABLES_PATH)
              .withQueryParam("catalog_name", catalogName)
              .withQueryParam("include_browse", "true")
              .withQueryParam("max_results", String.valueOf(SERVER_PAGE_SIZE))
              .withQueryParam("schema_name", schemaName);
      if (!pageToken.isEmpty()) {
        request.withQueryParam("page_token", pageToken);
      }

      JsonNode page = executeJson(handle, request);
      for (JsonNode table : page.path("tables")) {
        tables.add(
            new ListedTable(
                SDK_MAPPER.treeToValue(table, TableInfo.class).setProperties(null),
                table.path("securable_kind").asText(""),
                table.path("properties").path(TOTAL_SIZE_PROPERTY).asText(null)));
      }
      pageToken = page.path("next_page_token").asText("");
    } while (!pageToken.isEmpty());
    return tables.build();
  }

  /** Lists the tables of a schema without their columns and properties. */
  private static ImmutableList<TableInfo> listTableNamesAndTypes(
      DatabricksHandle handle, String catalogName, String schemaName) {
    return ImmutableList.copyOf(
        handle
            .client()
            .tables()
            .list(
                new ListTablesRequest()
                    .setCatalogName(catalogName)
                    .setSchemaName(schemaName)
                    .setIncludeBrowse(true)
                    .setMaxResults(SERVER_PAGE_SIZE)
                    .setOmitColumns(true)
                    .setOmitProperties(true)));
  }

  /**
   * Returns the JSON response of the {@code tables.get} call of a table. The SDK call {@code
   * tables().get(GetTableRequest)} isn't used because it doesn't return all the constraints, which
   * may be an SDK issue: the REST API returns UNIQUE constraints as {@code unique_constraint}, but
   * the {@code TableConstraint} of the SDK {@link TableInfo} (SDK 0.163.0) has fields only for
   * {@code primary_key_constraint}, {@code foreign_key_constraint} and {@code
   * named_table_constraint}. The SDK ignores unknown fields, so a UNIQUE constraint comes back as
   * an empty {@code TableConstraint}, without its name or columns. The request is the one of {@code
   * TablesAPI.get}, except that the full name is escaped as a path segment, so that names with
   * characters such as {@code #} or {@code ?} are read too.
   */
  static JsonNode getTableJson(DatabricksHandle handle, TableName table) throws IOException {
    // The query is in the URL because Request.getUri() decodes the path when adding query
    // parameters, which would undo the %23 and %3F escaping.
    return executeJson(
        handle,
        new Request(
            Request.GET,
            TABLES_PATH
                + "/"
                + urlPathSegmentEscaper().escape(table.fullName())
                + "?include_browse=true"));
  }

  /** Executes a GET request with the headers of the SDK calls and returns its JSON response. */
  private static JsonNode executeJson(DatabricksHandle handle, Request request) throws IOException {
    ApiClient apiClient = handle.client().apiClient();
    request.withHeader("Accept", "application/json");
    if (apiClient.workspaceId() != null) {
      request.withHeader("X-Databricks-Workspace-Id", apiClient.workspaceId());
    }

    try (InputStream body = apiClient.execute(request, InputStream.class)) {
      return JSON_READER.readTree(body);
    }
  }

  /** Prints the rows of a {@code tables.list} table across the five table CSV files. */
  private static void printTable(
      ListedTable listedTable,
      CSVPrinter tables,
      CSVPrinter columns,
      CSVPrinter views,
      CSVPrinter columnMasks,
      CSVPrinter rowFilters)
      throws IOException {
    TableInfo table = listedTable.info;
    tables.printRecord(
        table.getCatalogName(),
        table.getSchemaName(),
        table.getName(),
        table.getTableType(),
        table.getDataSourceFormat(),
        table.getStorageLocation(),
        listedTable.securableKind,
        table.getPipelineId(),
        listedTable.sizeInBytes);
    if (table.getViewDefinition() != null) {
      views.printRecord(
          table.getCatalogName(),
          table.getSchemaName(),
          table.getName(),
          table.getViewDefinition());
    }

    TableRowFilter rowFilter = table.getRowFilter();
    if (rowFilter != null) {
      rowFilters.printRecord(
          table.getCatalogName(),
          table.getSchemaName(),
          table.getName(),
          rowFilter.getFunctionName(),
          joinOrEmpty(", ", rowFilter.getInputColumnNames()));
    }

    if (table.getColumns() != null) {
      for (ColumnInfo column : table.getColumns()) {
        columns.printRecord(columnRecord(table, column));
        ColumnMask mask = column.getMask();
        if (mask != null) {
          columnMasks.printRecord(
              table.getCatalogName(),
              table.getSchemaName(),
              table.getName(),
              column.getName(),
              mask.getFunctionName(),
              joinOrEmpty(",", mask.getUsingColumnNames()));
        }
      }
    }
  }

  /** Returns the record of {@code columns.csv} for a column of a {@code tables.list} table. */
  private static Object[] columnRecord(TableInfo table, ColumnInfo column) {
    Object[] record = new Object[COLUMNS_RECORD_SIZE];
    record[ColumnsFormat.Header.TableCatalog.ordinal()] = table.getCatalogName();
    record[ColumnsFormat.Header.TableSchema.ordinal()] = table.getSchemaName();
    record[ColumnsFormat.Header.TableName.ordinal()] = table.getName();
    record[ColumnsFormat.Header.OrdinalPosition.ordinal()] = column.getPosition();
    record[ColumnsFormat.Header.ColumnName.ordinal()] = column.getName();
    record[ColumnsFormat.Header.DataType.ordinal()] = column.getTypeName();
    record[ColumnsFormat.Header.FullDataType.ordinal()] = column.getTypeText();
    record[ColumnsFormat.Header.IsNullable.ordinal()] =
        Boolean.FALSE.equals(column.getNullable()) ? "NO" : "YES";
    // The REST API sends type_precision and type_scale as 0, even for DECIMAL columns, and has no
    // character length, so they are read from type_text, such as decimal(18,4) or varchar(16). The
    // REST API has no datetime precision.
    Matcher decimal = DECIMAL_TYPE_PATTERN.matcher(Strings.nullToEmpty(column.getTypeText()));
    if (decimal.matches()) {
      record[ColumnsFormat.Header.NumericPrecision.ordinal()] = decimal.group(1);
      record[ColumnsFormat.Header.NumericScale.ordinal()] = decimal.group(2);
    }

    Matcher characters = CHAR_TYPE_PATTERN.matcher(Strings.nullToEmpty(column.getTypeText()));
    if (characters.matches()) {
      record[ColumnsFormat.Header.CharacterMaximumLength.ordinal()] = characters.group(1);
    }

    record[ColumnsFormat.Header.Comment.ordinal()] = column.getComment();
    record[ColumnsFormat.Header.PartitionIndex.ordinal()] = column.getPartitionIndex();
    record[ColumnsFormat.Header.TypeJson.ordinal()] = column.getTypeJson();
    // The default, generated and identity values come from type_json.metadata. They stay empty when
    // the column has no type_json.
    if (!Strings.isNullOrEmpty(column.getTypeJson())) {
      JsonNode metadata = parseTypeJsonMetadata(column.getTypeJson());
      String generationExpression = metadata.path("delta.generationExpression").asText("");
      String identityStart = metadata.path("delta.identity.start").asText("");
      record[ColumnsFormat.Header.ColumnDefault.ordinal()] =
          metadata.path("CURRENT_DEFAULT").asText("");
      record[ColumnsFormat.Header.IsGenerated.ordinal()] =
          generationExpression.isEmpty() ? "NEVER" : "ALWAYS";
      record[ColumnsFormat.Header.GenerationExpression.ordinal()] = generationExpression;
      record[ColumnsFormat.Header.IsIdentity.ordinal()] = identityStart.isEmpty() ? "NO" : "YES";
      if (!identityStart.isEmpty()) {
        record[ColumnsFormat.Header.IdentityGeneration.ordinal()] =
            metadata.path("delta.identity.allowExplicitInsert").asBoolean(false)
                ? "BY DEFAULT"
                : "ALWAYS";
        record[ColumnsFormat.Header.IdentityStart.ordinal()] = identityStart;
        record[ColumnsFormat.Header.IdentityIncrement.ordinal()] =
            metadata.path("delta.identity.step").asText("");
      }
    }
    return record;
  }

  /**
   * Prints a row for each primary key, foreign key and UNIQUE constraint of the {@code
   * table_constraints} of a {@code tables.get} response, skipping the other constraints, and a row
   * for each CHECK constraint of its {@code properties}, which {@code table_constraints} doesn't
   * have.
   */
  private static void printConstraints(CSVPrinter printer, TableName table, JsonNode tableJson)
      throws IOException {
    for (JsonNode constraint : tableJson.path("table_constraints")) {
      if (constraint.has("primary_key_constraint")) {
        printer.printRecord(
            constraintRecord(table, "PRIMARY KEY", constraint.path("primary_key_constraint")));
      } else if (constraint.has("foreign_key_constraint")) {
        printer.printRecord(
            constraintRecord(table, "FOREIGN KEY", constraint.path("foreign_key_constraint")));
      } else if (constraint.has("unique_constraint")) {
        printer.printRecord(
            constraintRecord(table, "UNIQUE", constraint.path("unique_constraint")));
      }
    }

    printCheckConstraints(
        printer, table.catalog, table.schema, table.name, tableJson.path("properties"));
  }

  /**
   * Returns the record of {@code table_constraints.csv} for one key constraint of {@code table}.
   */
  private static Object[] constraintRecord(TableName table, String constraintType, JsonNode key)
      throws IOException {
    // A foreign key has the full name of its parent table, such as main.sales.orders.
    String parentTable = key.path("parent_table").asText("");
    String referencedCatalog = "";
    String referencedSchema = "";
    String referencedTable = parentTable;
    List<String> parentTableParts = PARENT_TABLE_SPLITTER.splitToList(parentTable);
    if (parentTableParts.size() == 3) {
      referencedCatalog = parentTableParts.get(0);
      referencedSchema = parentTableParts.get(1);
      referencedTable = parentTableParts.get(2);
    }

    Object[] record = new Object[TABLE_CONSTRAINTS_RECORD_SIZE];
    record[TableConstraintsFormat.Header.ConstraintCatalog.ordinal()] = table.catalog;
    record[TableConstraintsFormat.Header.ConstraintSchema.ordinal()] = table.schema;
    record[TableConstraintsFormat.Header.ConstraintName.ordinal()] = key.path("name").asText("");
    record[TableConstraintsFormat.Header.TableCatalog.ordinal()] = table.catalog;
    record[TableConstraintsFormat.Header.TableSchema.ordinal()] = table.schema;
    record[TableConstraintsFormat.Header.TableName.ordinal()] = table.name;
    record[TableConstraintsFormat.Header.ConstraintType.ordinal()] = constraintType;
    record[TableConstraintsFormat.Header.ConstraintColumns.ordinal()] =
        jsonArrayOrEmpty(key.path("child_columns"));
    record[TableConstraintsFormat.Header.ReferencedTableCatalog.ordinal()] = referencedCatalog;
    record[TableConstraintsFormat.Header.ReferencedTableSchema.ordinal()] = referencedSchema;
    record[TableConstraintsFormat.Header.ReferencedTableName.ordinal()] = referencedTable;
    record[TableConstraintsFormat.Header.ReferencedColumns.ordinal()] =
        jsonArrayOrEmpty(key.path("parent_columns"));
    record[TableConstraintsFormat.Header.Rely.ordinal()] = key.path("rely").asBoolean(false);
    return record;
  }

  /** Returns the row of a table whose {@code tables.get} call failed: its names and ERROR. */
  private static Object[] constraintErrorRecord(TableName table) {
    Object[] record = new Object[TABLE_CONSTRAINTS_RECORD_SIZE];
    record[TableConstraintsFormat.Header.ConstraintCatalog.ordinal()] = table.catalog;
    record[TableConstraintsFormat.Header.ConstraintSchema.ordinal()] = table.schema;
    record[TableConstraintsFormat.Header.TableCatalog.ordinal()] = table.catalog;
    record[TableConstraintsFormat.Header.TableSchema.ordinal()] = table.schema;
    record[TableConstraintsFormat.Header.TableName.ordinal()] = table.name;
    record[TableConstraintsFormat.Header.ConstraintType.ordinal()] = ERROR_TYPE;
    return record;
  }

  /** Returns {@code type_json.metadata}, or a missing node when {@code typeJson} isn't JSON. */
  private static JsonNode parseTypeJsonMetadata(String typeJson) {
    try {
      return JSON_READER.readTree(typeJson).path("metadata");
    } catch (IOException e) {
      logger.debug("Failed to parse ColumnInfo.type_json: {}", e.getMessage());
      return MissingNode.getInstance();
    }
  }

  /** Returns {@code node} as JSON if it's an array, such as {@code ["id"]}, or empty otherwise. */
  private static String jsonArrayOrEmpty(JsonNode node) throws IOException {
    return node.isArray() ? JSON_WRITER.writeValueAsString(node) : "";
  }

  private static String joinOrEmpty(String separator, Collection<String> values) {
    return values == null ? "" : String.join(separator, values);
  }

  /**
   * Logs and returns a warning for the task summary about the objects with only the BROWSE
   * privilege, for which the REST API leaves out {@code missingDetails}, or returns null when there
   * are none. {@code browseOnlyCatalogs} has the catalog name of each browse-only object, and
   * {@code objectCount} counts all the objects.
   */
  @Nullable
  private static String browseOnlyWarning(
      Multiset<String> browseOnlyCatalogs,
      int objectCount,
      String objects,
      String missingDetails,
      String privilege) {
    if (browseOnlyCatalogs.isEmpty()) {
      return null;
    }

    String warning =
        String.format(
            "%d of %d %s are browse-only, so their %s are missing until USE CATALOG, USE SCHEMA"
                + " and %s are granted",
            browseOnlyCatalogs.size(), objectCount, objects, missingDetails, privilege);
    logger.warn("{}. Browse-only {} per catalog: {}", warning, objects, browseOnlyCatalogs);
    return warning;
  }

  /** The names of a schema listed by {@code schemas.list}. */
  private static final class SchemaName {
    final String catalog;
    final String name;

    SchemaName(String catalog, String name) {
      this.catalog = catalog;
      this.name = name;
    }

    /** Returns the quoted name of the schema, such as {@code `main`.`sales`}. */
    @Override
    public String toString() {
      return quoteQualifiedName(catalog, name);
    }
  }

  /**
   * A table listed by {@code tables.list}, with its {@code securable_kind}, such as {@code
   * TABLE_DELTA}, which the SDK {@link TableInfo} doesn't have, and its {@code
   * spark.sql.statistics.totalSize} property, which is null when no statistics were computed or the
   * table is browse-only.
   */
  private static final class ListedTable {
    final TableInfo info;
    final String securableKind;
    final String sizeInBytes;

    ListedTable(TableInfo info, String securableKind, String sizeInBytes) {
      this.info = info;
      this.securableKind = securableKind;
      this.sizeInBytes = sizeInBytes;
    }
  }

  /** The names of a table listed by {@code tables.list}. */
  static final class TableName {
    final String catalog;
    final String schema;
    final String name;

    TableName(String catalog, String schema, String name) {
      this.catalog = catalog;
      this.schema = schema;
      this.name = name;
    }

    /** Returns the full name of the table, such as {@code main.sales.orders}. */
    String fullName() {
      return catalog + "." + schema + "." + name;
    }

    /** Returns the quoted name of the table, such as {@code `main`.`sales`.`orders`}. */
    @Override
    public String toString() {
      return quoteQualifiedName(catalog, schema, name);
    }
  }
}
