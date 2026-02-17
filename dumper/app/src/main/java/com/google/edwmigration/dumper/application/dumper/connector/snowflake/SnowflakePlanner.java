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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ResultSetTransformer;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.DatabasesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.ExternalTablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.FunctionInfoFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.SchemataFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.TableStorageMetricsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.TablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.UserDefinedFunctionsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.WarehousesFormat;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * The generator of task lists for Snowflake connectors.
 *
 * <p>The generated lists consist of tasks that serve a single purpose (e.g. Assessment metadata
 * tasks).
 */
@ParametersAreNonnullByDefault
final class SnowflakePlanner {

  public static final AssessmentQuery SHOW_EXTERNAL_TABLES =
      AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES);

  private enum Format {
    CORTEX_AI_SEARCH("cortex_ai_search-au.csv"),
    CORTEX_AI_SQL("cortex_ai_sql-au.csv"),
    CORTEX_ANALYST("cortex_analyst-au.csv"),
    DATA_TRANSFER("data_transfer-au.csv"),
    DOCUMENT_AI("document_ai-au.csv"),
    EXTERNAL_TABLES(ExternalTablesFormat.AU_ZIP_ENTRY_NAME),
    FUNCTION_INFO(FunctionInfoFormat.AU_ZIP_ENTRY_NAME),
    HYBRID_TABLE_USAGE("hybrid_table_usage-au.csv"),
    SEARCH_OPTIMIZATION("search_optimization-au.csv"),
    SNOWPIPE_STREAMING("snowpipe_streaming-au.csv"),
    STAGE_STORAGE_USAGE("stage_storage_usage-au.csv"),
    TABLE_STORAGE_METRICS(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME),
    USER_DEFINED_FUNCTIONS(UserDefinedFunctionsFormat.IS_ZIP_ENTRY_NAME),
    VIEW_REFRESH("view_refresh-au.csv"),
    WAREHOUSES(WarehousesFormat.AU_ZIP_ENTRY_NAME);

    private final String value;

    Format(String value) {
      this.value = value;
    }
  }

  private static final String TIME_PREDICATE =
      "timestamp > CURRENT_TIMESTAMP(0) - INTERVAL '14 days'";

  private final ImmutableList<AssessmentQuery> assessmentQueries =
      ImmutableList.of(
          AssessmentQuery.createCortexAiSearchSelect(),
          AssessmentQuery.createCortexAiSqlSelect(),
          AssessmentQuery.createCortexAnalyst(),
          AssessmentQuery.createDataTransferSelect(),
          AssessmentQuery.createDocumentAiSelect(),
          AssessmentQuery.createHybridTableSelect(),
          AssessmentQuery.createMetricsSelect(Format.TABLE_STORAGE_METRICS),
          AssessmentQuery.createSearchOptimizationSelect(),
          AssessmentQuery.createShow("FUNCTIONS", Format.FUNCTION_INFO),
          AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES),
          AssessmentQuery.createSnowpipeSelect(),
          AssessmentQuery.createStageStorageSelect(),
          AssessmentQuery.createUserDefinedFunctionsSelect(),
          AssessmentQuery.createViewRefreshSelect(),
          SHOW_EXTERNAL_TABLES);

  private final ImmutableList<AssessmentQuery> liteAssessmentQueries =
      ImmutableList.of(
          AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES),
          AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES),
          AssessmentQuery.createShow("FUNCTIONS", Format.FUNCTION_INFO));

  AssessmentQuery externalTablesInDatabase(String quotedDatabaseName) {
    String query = String.format("SHOW EXTERNAL TABLES IN DATABASE %s", quotedDatabaseName);
    String zipEntryName = Format.EXTERNAL_TABLES.value;
    return new AssessmentQuery(false, query, zipEntryName, LOWER_UNDERSCORE);
  }

  ImmutableList<AssessmentQuery> generateAssessmentQueries() {
    return assessmentQueries;
  }

  ImmutableList<Task<?>> generateLiteSpecificQueries() {
    String view = "SNOWFLAKE.ACCOUNT_USAGE";
    String filter = " WHERE DELETED IS NULL";
    ImmutableList.Builder<Task<?>> builder = ImmutableList.builder();

    String databases =
        String.format("SELECT database_name, database_owner FROM %s.DATABASES%s", view, filter);
    builder.add(
        new JdbcSelectTask(DatabasesFormat.AU_ZIP_ENTRY_NAME, databases)
            .withHeaderClass(DatabasesFormat.Header.class));

    String schemata =
        String.format("SELECT catalog_name, schema_name FROM %s.SCHEMATA%s", view, filter);
    builder.add(
        new JdbcSelectTask(SchemataFormat.AU_ZIP_ENTRY_NAME, schemata)
            .withHeaderClass(SchemataFormat.Header.class));

    String tables =
        String.format(
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
                + " clustering_key FROM %s.TABLES%s",
            view, filter);
    builder.add(
        new JdbcSelectTask(TablesFormat.AU_ZIP_ENTRY_NAME, tables)
            .withHeaderClass(TablesFormat.Header.class));
    builder.add(procedures.toTask());
    builder.add(reportDateRange.toTask());
    builder.add(eventState.toTask());
    builder.add(warehouseEventsHistory.toTask());
    builder.add(warehouseMetering.toTask());
    builder.add(storageMetrics.toTask());

    for (AssessmentQuery item : liteAssessmentQueries) {
      String query = String.format(item.formatString, view, /* an empty WHERE clause */ "");
      String zipName = item.zipEntryName;
      Task<?> task = new JdbcSelectTask(zipName, query).withHeaderTransformer(item.transformer());
      builder.add(task);
    }
    return builder.build();
  }

  static class AssessmentQuery {
    final boolean needsOverride;
    final String formatString;
    final String zipEntryName;
    private final CaseFormat caseFormat;

    private AssessmentQuery(
        boolean needsOverride, String formatString, String zipEntryName, CaseFormat caseFormat) {
      this.needsOverride = needsOverride;
      this.formatString = formatString;
      this.zipEntryName = zipEntryName;
      this.caseFormat = caseFormat;
    }

    static AssessmentQuery createCortexAiSearchSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE usage_date > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.CORTEX_AI_SEARCH.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createCortexAiSqlSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AISQL_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE usage_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.CORTEX_AI_SQL.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createCortexAnalyst() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.CORTEX_ANALYST.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createDataTransferSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.DATA_TRANSFER.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createDocumentAiSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.DOCUMENT_AI_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.DOCUMENT_AI.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createHybridTableSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.HYBRID_TABLE_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.HYBRID_TABLE_USAGE.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createViewRefreshSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.VIEW_REFRESH.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createMetricsSelect(Format zipFormat) {
      String formatString = "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s";
      return new AssessmentQuery(true, formatString, zipFormat.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createSearchOptimizationSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.SNOWPIPE_STREAMING.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createSnowpipeSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_CLIENT_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.SNOWPIPE_STREAMING.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createStageStorageSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE usage_date > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.STAGE_STORAGE_USAGE.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createUserDefinedFunctionsSelect() {
      String formatValue = Format.USER_DEFINED_FUNCTIONS.value;
      String formatString =
          "SELECT FUNCTION_CATALOG, FUNCTION_SCHEMA, FUNCTION_NAME, FUNCTION_LANGUAGE, ARGUMENT_SIGNATURE, "
              + "FUNCTION_OWNER, COMMENT, VOLATILITY, RUNTIME_VERSION, LAST_ALTERED, "
              + "PACKAGES, IMPORTS, FUNCTION_DEFINITION, IS_AGGREGATE, IS_DATA_METRIC, IS_MEMOIZABLE "
              + "FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS "
              + "WHERE FUNCTION_SCHEMA != 'INFORMATION_SCHEMA' ";
      return new AssessmentQuery(false, formatString, formatValue, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createShow(String view, Format zipFormat) {
      String queryString = String.format("SHOW %s", view);
      return new AssessmentQuery(false, queryString, zipFormat.value, LOWER_UNDERSCORE);
    }

    ResultSetTransformer<String[]> transformer() {
      return HeaderTransformerUtil.toCamelCaseFrom(caseFormat);
    }

    String substitute(String schema, String whereCondition) {
      return String.format(formatString, schema, whereCondition);
    }
  }

  private static final class LiteTaskData {

    final String csvFile;
    final String query;
    final ImmutableList<String> header;

    LiteTaskData(String csv, String query, ImmutableList<String> header) {
      this.csvFile = csv;
      this.query = query;
      this.header = header;
    }

    Task<?> toTask() {
      return new LiteTimeSeriesTask(csvFile, query, header);
    }
  }

  private static LiteTaskData eventState = initEventState();
  private static LiteTaskData procedures = initProcedures();
  private static LiteTaskData reportDateRange = initReportDateRange();
  private static LiteTaskData storageMetrics = initStorageMetrics();
  private static LiteTaskData warehouseEventsHistory = initWarehouseEventsHistory();
  private static LiteTaskData warehouseMetering = initWarehouseMetering();

  private static String buildQuery(String selectList, String view, String predicate) {
    return String.format("SELECT %s FROM %s WHERE %s", selectList, view, predicate);
  }

  private static LiteTaskData initEventState() {
    String query =
        "SELECT event_state, count(*)"
            + " FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY"
            + " WHERE event_name LIKE '%CLUSTER%' GROUP BY ALL";
    ImmutableList<String> header = ImmutableList.of("EventState", "Count");
    return new LiteTaskData("event_state.csv", query, header);
  }

  private static LiteTaskData initProcedures() {
    String view = "SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES";
    String query =
        String.format(
            "SELECT procedure_language, procedure_owner, count(1) FROM %s GROUP BY ALL", view);
    ImmutableList<String> header = ImmutableList.of("Language", "Owner", "Count");
    return new LiteTaskData("procedures.csv", query, header);
  }

  private static LiteTaskData initReportDateRange() {
    String selectList = "min(timestamp), max(timestamp)";
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY";
    String query = buildQuery(selectList, view, TIME_PREDICATE);
    ImmutableList<String> header = ImmutableList.of("StartTime", "EndTime");
    return new LiteTaskData("report_date_range.csv", query, header);
  }

  private static LiteTaskData initStorageMetrics() {
    String selectList =
        String.join(
            ", ",
            "table_catalog",
            "table_schema_id",
            "table_schema",
            "table_name",
            "id",
            "clone_group_id",
            "is_transient",
            "active_bytes",
            "time_travel_bytes",
            "failsafe_bytes",
            "retained_for_clone_bytes",
            "table_created",
            "table_dropped",
            "table_entered_failsafe",
            "catalog_created",
            "catalog_dropped",
            "schema_created",
            "schema_dropped",
            "comment",
            "deleted");
    String view = "SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS";
    String predicate = "deleted = FALSE AND schema_dropped IS NULL AND table_dropped IS NULL";
    String query = String.format("SELECT %s FROM %s WHERE %s", selectList, view, predicate);
    ImmutableList<String> header =
        ImmutableList.of(
            "TableCatalog",
            "TableSchemaId",
            "TableSchema",
            "TableName",
            "Id",
            "CloneGroupId",
            "IsTransient",
            "ActiveBytes",
            "TimeTravelBytes",
            "FailsafeBytes",
            "RetainedForCloneBytes",
            "TableCreated",
            "TableDropped",
            "TableEnteredFailsafe",
            "CatalogCreated",
            "CatalogDropped",
            "SchemaCreated",
            "SchemaDropped",
            "Comment",
            "Deleted");
    return new LiteTaskData("table_storage_metrics-au.csv", query, header);
  }

  private static LiteTaskData initWarehouseEventsHistory() {
    String selectList =
        String.join(
            ", ",
            "timestamp",
            "warehouse_id",
            "warehouse_name",
            "cluster_number",
            "event_name",
            "event_reason",
            "event_state",
            "query_id");
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY";
    String query = buildQuery(selectList, view, TIME_PREDICATE);
    ImmutableList<String> header =
        ImmutableList.of(
            "Timestamp",
            "WarehouseId",
            "WarehouseName",
            "ClusterNumber",
            "EventName",
            "EventReason",
            "EventState",
            "QueryId");
    return new LiteTaskData("warehouse_events_lite.csv", query, header);
  }

  private static LiteTaskData initWarehouseMetering() {
    String selectList =
        String.join(
            ", ",
            "start_time",
            "end_time",
            "warehouse_name",
            "credits_used_compute",
            "credits_used_cloud_services");
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY";
    String query =
        buildQuery(
            selectList,
            view,
            "end_time IS NULL OR end_time > CURRENT_TIMESTAMP(0) - INTERVAL '14 days'");
    ImmutableList<String> header =
        ImmutableList.of(
            "StartTime",
            "EndTime",
            "WarehouseName",
            "CreditsUsedCompute",
            "CreditsUsedCloudServices");
    return new LiteTaskData("warehouse_metering_lite.csv", query, header);
  }
}
