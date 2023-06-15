/*
 * Copyright 2022-2023 Google LLC
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

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import com.google.errorprone.annotations.ForOverride;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector to Snowflake databases.
 *
 * @author matt
 */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Snowflake.")
public class SnowflakeMetadataConnector extends AbstractSnowflakeConnector
    implements MetadataConnector, SnowflakeMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeMetadataConnector.class);

  public enum SnowflakeMetadataConnectorProperties implements ConnectorProperty {
    DATABASES_OVERRIDE_QUERY,
    DATABASES_OVERRIDE_WHERE,
    SCHEMATA_OVERRIDE_QUERY,
    SCHEMATA_OVERRIDE_WHERE,
    TABLES_OVERRIDE_QUERY,
    TABLES_OVERRIDE_WHERE,
    COLUMNS_OVERRIDE_QUERY,
    COLUMNS_OVERRIDE_WHERE,
    VIEWS_OVERRIDE_QUERY,
    VIEWS_OVERRIDE_WHERE,
    FUNCTIONS_OVERRIDE_QUERY,
    FUNCTIONS_OVERRIDE_WHERE,
    WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY,
    WAREHOUSE_EVENTS_HISTORY_OVERRIDE_WHERE,
    AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY,
    AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_WHERE,
    COPY_HISTORY_OVERRIDE_QUERY,
    COPY_HISTORY_OVERRIDE_WHERE,
    DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY,
    DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_WHERE,
    LOGIN_HISTORY_OVERRIDE_QUERY,
    LOGIN_HISTORY_OVERRIDE_WHERE,
    METERING_DAILY_HISTORY_OVERRIDE_QUERY,
    METERING_DAILY_HISTORY_OVERRIDE_WHERE,
    PIPE_USAGE_HISTORY_OVERRIDE_QUERY,
    PIPE_USAGE_HISTORY_OVERRIDE_WHERE,
    QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY,
    QUERY_ACCELERATION_HISTORY_OVERRIDE_WHERE,
    QUERY_HISTORY_OVERRIDE_QUERY,
    QUERY_HISTORY_OVERRIDE_WHERE,
    REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY,
    REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_WHERE,
    SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY,
    SERVERLESS_TASK_HISTORY_OVERRIDE_WHERE,
    SHOW_WAREHOUSES_OVERRIDE_QUERY,
    SHOW_WAREHOUSES_OVERRIDE_WHERE,
    TABLE_STORAGE_METRICS_OVERRIDE_QUERY,
    TABLE_STORAGE_METRICS_OVERRIDE_WHERE,
    TASK_HISTORY_OVERRIDE_QUERY,
    TASK_HISTORY_OVERRIDE_WHERE,
    WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY,
    WAREHOUSE_LOAD_HISTORY_OVERRIDE_WHERE,
    WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY,
    WAREHOUSE_METERING_HISTORY_OVERRIDE_WHERE;

    private final String name;
    private final String description;

    SnowflakeMetadataConnectorProperties() {
      boolean isWhere = name().endsWith("WHERE");
      String name = name().split("_")[0].toLowerCase();
      this.name = "snowflake.metadata." + name + (isWhere ? ".where" : ".query");
      this.description =
          isWhere
              ? "Custom where condition to append to query for metadata " + name + " dump."
              : "Custom query for metadata " + name + " dump.";
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }
  }

  protected SnowflakeMetadataConnector(@Nonnull String name) {
    super(name);
  }

  public SnowflakeMetadataConnector() {
    this("snowflake");
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return SnowflakeMetadataConnectorProperties.class;
  }

  protected static class TaskVariant {

    public final String zipEntryName;
    public final String schemaName;
    public final String whereClause;

    public TaskVariant(String zipEntryName, String schemaName, String whereClause) {
      this.zipEntryName = zipEntryName;
      this.schemaName = schemaName;
      this.whereClause = whereClause;
    }

    public TaskVariant(String zipEntryName, String schemaName) {
      this(zipEntryName, schemaName, "");
    }
  }

  /** Adds the INFORMATION_SCHEMA task, with a fallback to the ACCOUNT_USAGE task. */
  @ForOverride
  protected void addSqlTasks(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull TaskVariant is_task,
      @Nonnull TaskVariant au_task,
      ConnectorArguments arguments) {
    AbstractJdbcTask<Summary> is_jdbcTask =
        new JdbcSelectTask(
                is_task.zipEntryName,
                String.format(format, is_task.schemaName, is_task.whereClause))
            .withHeaderClass(header);

    AbstractJdbcTask<Summary> au_jdbcTask =
        new JdbcSelectTask(
                au_task.zipEntryName,
                String.format(format, au_task.schemaName, au_task.whereClause))
            .withHeaderClass(header);

    if (arguments.isAssessment()) {
      out.add(au_jdbcTask);
    } else {
      out.add(is_jdbcTask);
      out.add(au_jdbcTask.onlyIfFailed(is_jdbcTask));
    }
  }

  @ForOverride
  protected void addSingleSqlTask(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull TaskVariant task) {
    out.add(
        new JdbcSelectTask(
                task.zipEntryName, String.format(format, task.schemaName, task.whereClause))
            .withHeaderClass(header));
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    boolean INJECT_IS_FAULT = arguments.isTestFlag('A');
    final String IS = INJECT_IS_FAULT ? "__NONEXISTENT__" : "INFORMATION_SCHEMA";
    final String AU = "SNOWFLAKE.ACCOUNT_USAGE";
    final String AU_WHERE = " WHERE DELETED IS NULL";

    // Docref: https://docs.snowflake.net/manuals/sql-reference/info-schema.html#list-of-views
    // ACCOUNT_USAGE is much faster than INFORMATION_SCHEMA and does not have the size limitations,
    // but requires extra privileges to be granted.
    // https://docs.snowflake.net/manuals/sql-reference/account-usage.html
    // https://docs.snowflake.net/manuals/user-guide/data-share-consumers.html
    // You must: GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE <SOMETHING>;
    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.DatabasesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT database_name, database_owner FROM %1$s.DATABASES%2$s",
            SnowflakeMetadataConnectorProperties.DATABASES_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.DATABASES_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.DatabasesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(
            SnowflakeMetadataDumpFormat.DatabasesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.SchemataFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
            SnowflakeMetadataConnectorProperties.SCHEMATA_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.SCHEMATA_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.SchemataFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SnowflakeMetadataDumpFormat.SchemataFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.TablesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
                + " clustering_key FROM %1$s.TABLES%2$s",
            SnowflakeMetadataConnectorProperties.TABLES_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.TABLES_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.TablesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SnowflakeMetadataDumpFormat.TablesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments); // Painfully slow.

    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.ColumnsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name,"
                + " data_type FROM %1$s.COLUMNS%2$s",
            SnowflakeMetadataConnectorProperties.COLUMNS_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.COLUMNS_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.ColumnsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SnowflakeMetadataDumpFormat.ColumnsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments); // Very fast.

    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.ViewsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, view_definition FROM %1$s.VIEWS%2$s",
            SnowflakeMetadataConnectorProperties.VIEWS_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.VIEWS_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.ViewsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SnowflakeMetadataDumpFormat.ViewsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasks(
        out,
        SnowflakeMetadataDumpFormat.FunctionsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT function_schema, function_name, data_type, argument_signature FROM"
                + " %1$s.FUNCTIONS%2$s",
            SnowflakeMetadataConnectorProperties.FUNCTIONS_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.FUNCTIONS_OVERRIDE_WHERE),
        new TaskVariant(SnowflakeMetadataDumpFormat.FunctionsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(
            SnowflakeMetadataDumpFormat.FunctionsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    if (arguments.isAssessment()) {
      addAssessmentTasks(out, arguments, AU);
    }
  }

  private void addAssessmentTasks(
      List<? super Task<?>> out, ConnectorArguments arguments, String accountUsage) {
    addSingleSqlTask(
        out,
        WarehouseEventsHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.WAREHOUSE_EVENTS_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.WAREHOUSE_EVENTS_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(WarehouseEventsHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        AutomaticClusteringHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.AUTOMATIC_CLUSTERING_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(AutomaticClusteringHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        CopyHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.COPY_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.COPY_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.COPY_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(CopyHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        DatabaseReplicationUsageHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.DATABASE_REPLICATION_USAGE_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(DatabaseReplicationUsageHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        LoginHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.LOGIN_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.LOGIN_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.LOGIN_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(LoginHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        MeteringDailyHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.METERING_DAILY_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.METERING_DAILY_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.METERING_DAILY_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(MeteringDailyHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        PipeUsageHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.PIPE_USAGE_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.PIPE_USAGE_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.PIPE_USAGE_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(PipeUsageHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        QueryAccelerationHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.QUERY_ACCELERATION_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.QUERY_ACCELERATION_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(QueryAccelerationHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        QueryHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.QUERY_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.QUERY_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.QUERY_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(QueryHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        ReplicationGroupUsageHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.REPLICATION_GROUP_USAGE_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(ReplicationGroupUsageHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        ServerlessTaskHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.SERVERLESS_TASK_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.SERVERLESS_TASK_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(ServerlessTaskHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        TableStorageMetricsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s",
            SnowflakeMetadataConnectorProperties.TABLE_STORAGE_METRICS_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.TABLE_STORAGE_METRICS_OVERRIDE_WHERE),
        new TaskVariant(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        TaskHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.TASK_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.TASK_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.TASK_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(TaskHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        WarehouseLoadHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.WAREHOUSE_LOAD_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.WAREHOUSE_LOAD_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(WarehouseLoadHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        WarehouseMeteringHistoryFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM %1$s.WAREHOUSE_METERING_HISTORY%2$s",
            SnowflakeMetadataConnectorProperties.WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.WAREHOUSE_METERING_HISTORY_OVERRIDE_WHERE),
        new TaskVariant(WarehouseMeteringHistoryFormat.AU_ZIP_ENTRY_NAME, accountUsage));

    addSingleSqlTask(
        out,
        ShowWarehousesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT * FROM (SHOW WAREHOUSES)%2$s",
            SnowflakeMetadataConnectorProperties.SHOW_WAREHOUSES_OVERRIDE_QUERY,
            SnowflakeMetadataConnectorProperties.SHOW_WAREHOUSES_OVERRIDE_WHERE),
        new TaskVariant(ShowWarehousesFormat.AU_ZIP_ENTRY_NAME, accountUsage));
  }

  private String getOverrideableQuery(
      @Nonnull ConnectorArguments arguments,
      @Nonnull String defaultSql,
      @Nonnull SnowflakeMetadataConnectorProperties queryProperty,
      @Nonnull SnowflakeMetadataConnectorProperties whereProperty) {

    String overrideQuery = arguments.getDefinition(queryProperty);
    if (overrideQuery != null) return overrideQuery;

    String overrideWhere = arguments.getDefinition(whereProperty);
    if (overrideWhere != null) return defaultSql + " WHERE " + overrideWhere;

    return defaultSql;
  }
}
