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
      AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES, LOWER_UNDERSCORE);

  private enum Format {
    EXTERNAL_TABLES(ExternalTablesFormat.AU_ZIP_ENTRY_NAME),
    FUNCTION_INFO(FunctionInfoFormat.AU_ZIP_ENTRY_NAME),
    TABLE_STORAGE_METRICS(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME),
    WAREHOUSES(WarehousesFormat.AU_ZIP_ENTRY_NAME);

    private final String value;

    Format(String value) {
      this.value = value;
    }
  }

  private static final String TIME_PREDICATE =
      "timestamp > CURRENT_TIMESTAMP(0) - INTERVAL '14 days'";

  ImmutableList<AssessmentQuery> generateAssessmentQueries() {
    return ImmutableList.of(
        AssessmentQuery.createMetricsSelect(Format.TABLE_STORAGE_METRICS, UPPER_UNDERSCORE),
        AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES, LOWER_UNDERSCORE),
        SHOW_EXTERNAL_TABLES,
        AssessmentQuery.createShow("FUNCTIONS", Format.FUNCTION_INFO, LOWER_UNDERSCORE));
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
    builder.add(proceduresTask());
    builder.add(reportDateRangeTask());
    builder.add(eventStateTask());
    builder.add(operationEndsTask());
    builder.add(operationStartsTask());
    builder.add(warehouseEventsHistoryTask());
    builder.add(warehouseMeteringTask());
    builder.add(storageMetricsLiteTask());

    ImmutableList<AssessmentQuery> liteAssessmentQueries =
        ImmutableList.of(
            AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES, LOWER_UNDERSCORE),
            AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES, LOWER_UNDERSCORE),
            AssessmentQuery.createShow("FUNCTIONS", Format.FUNCTION_INFO, LOWER_UNDERSCORE));

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

    AssessmentQuery withFormatString(String formatString) {
      return new AssessmentQuery(needsOverride, formatString, zipEntryName, caseFormat);
    }

    static AssessmentQuery createMetricsSelect(Format zipFormat, CaseFormat caseFormat) {
      String formatString = "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s";
      return new AssessmentQuery(true, formatString, zipFormat.value, caseFormat);
    }

    static AssessmentQuery createShow(String view, Format zipFormat, CaseFormat caseFormat) {
      String queryString = String.format("SHOW %s", view);
      return new AssessmentQuery(false, queryString, zipFormat.value, caseFormat);
    }

    ResultSetTransformer<String[]> transformer() {
      return HeaderTransformerUtil.toCamelCaseFrom(caseFormat);
    }
  }

  Task<?> eventStateTask() {
    String query =
        "SELECT event_state, count(*)"
            + " FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY"
            + " WHERE event_name LIKE '%CLUSTER%' GROUP BY ALL";
    ImmutableList<String> header = ImmutableList.of("EventState", "Count");
    return new LiteTimeSeriesTask("event_state.csv", query, header);
  }

  Task<?> operationEndsTask() {
    String selectList = "warehouse_name, cluster_number, event_state, timestamp";
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY";
    String predicate =
        "upper(event_name) LIKE '%SUSPEND%'"
            + " AND "
            + "upper(event_name) LIKE '%CLUSTER%'"
            + " AND "
            + TIME_PREDICATE;
    String query = buildQuery(selectList, view, predicate);
    ImmutableList<String> header =
        ImmutableList.of("WarehouseName", "ClusterNumber", "EventState", "Timestamp");
    return new LiteTimeSeriesTask("clusters_spin_downs.csv", query, header);
  }

  Task<?> operationStartsTask() {
    String selectList = "warehouse_name, cluster_number, event_state, timestamp";
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY";
    String predicate =
        "upper(event_name) LIKE '%RESUME%'"
            + " AND "
            + "upper(event_name) LIKE '%CLUSTER%'"
            + " AND "
            + TIME_PREDICATE;
    String query = buildQuery(selectList, view, predicate);
    ImmutableList<String> header =
        ImmutableList.of("WarehouseName", "ClusterNumber", "EventState", "Timestamp");
    return new LiteTimeSeriesTask("clusters_spin_ups.csv", query, header);
  }

  Task<?> proceduresTask() {
    String view = "SNOWFLAKE.ACCOUNT_USAGE.PROCEDURES";
    String query =
        String.format(
            "SELECT procedure_language, procedure_owner, count(1) FROM %s GROUP BY ALL", view);
    ImmutableList<String> header = ImmutableList.of("Language", "Owner", "Count");
    return new LiteTimeSeriesTask("procedures.csv", query, header);
  }

  Task<?> reportDateRangeTask() {
    String selectList = "min(timestamp), max(timestamp)";
    String view = "SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY";
    String query = buildQuery(selectList, view, TIME_PREDICATE);
    ImmutableList<String> header = ImmutableList.of("StartTime", "EndTime");
    return new LiteTimeSeriesTask("report_date_range.csv", query, header);
  }

  Task<?> storageMetricsLiteTask() {
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
    String predicate =
        "deleted IS FALSE" + " AND " + "schema_dropped IS NULL" + " AND " + "table_dropped IS NULL";
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
    return new LiteTimeSeriesTask("table_storage_metrics-au.csv", query, header);
  }

  private static String buildQuery(String selectList, String view, String predicate) {
    return String.format("SELECT %s FROM %s WHERE %s", selectList, view, predicate);
  }

  Task<?> warehouseEventsHistoryTask() {
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
    return new LiteTimeSeriesTask("warehouse_events_lite.csv", query, header);
  }

  Task<?> warehouseMeteringTask() {
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
    return new LiteTimeSeriesTask("warehouse_metering_lite.csv", query, header);
  }
}
