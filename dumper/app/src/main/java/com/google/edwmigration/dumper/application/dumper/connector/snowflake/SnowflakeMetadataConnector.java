/*
 * Copyright 2022-2024 Google LLC
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

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.USAGE_THEN_SCHEMA_SOURCE;

import com.google.auto.service.AutoService;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ResultSetTransformer;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.DatabasesFormat.Header;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector to Snowflake databases.
 *
 * @author matt
 */
@AutoService(Connector.class)
@Description("Dumps metadata from Snowflake.")
public class SnowflakeMetadataConnector extends AbstractSnowflakeConnector
    implements MetadataConnector, SnowflakeMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeMetadataConnector.class);

  public enum SnowflakeMetadataConnectorProperties {
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
    TABLE_STORAGE_METRICS_OVERRIDE_QUERY,
    TABLE_STORAGE_METRICS_OVERRIDE_WHERE;
  }

  static boolean isWhere(String name) {
    return name.endsWith("WHERE");
  }

  static String toNameComponent(String enumName) {
    return enumName.split("_")[0].toLowerCase();
  }

  static String toSuffix(String enumName) {
    return isWhere(enumName) ? ".where" : ".query";
  }

  static String toDescription(String enumName) {
    String name = toNameComponent(enumName);
    boolean isWhere = isWhere(enumName);
    return isWhere
        ? "Custom where condition to append to query for metadata " + name + " dump."
        : "Custom query for metadata " + name + " dump.";
  }

  static String toName(String enumName) {
    return "snowflake.metadata." + toNameComponent(enumName) + toSuffix(enumName);
  }

  static ConnectorProperty parseProperty(String enumName) {
    String name = toName(enumName);
    String description = toDescription(enumName);
    return new ConnectorProperty() {
      @Override
      @Nonnull
      public String getDescription() {
        return description;
      }

      @Override
      @Nonnull
      public String getName() {
        return name;
      }
    };
  }

  private final SnowflakeInput inputSource;

  SnowflakeMetadataConnector(@Nonnull String name, @Nonnull SnowflakeInput inputSource) {
    super(name);
    this.inputSource = inputSource;
  }

  public SnowflakeMetadataConnector() {
    this("snowflake", USAGE_THEN_SCHEMA_SOURCE);
  }

  @Override
  @Nonnull
  public Iterable<ConnectorProperty> getPropertyConstants() {
    ImmutableList.Builder<ConnectorProperty> builder = ImmutableList.builder();
    for (SnowflakeMetadataConnectorProperties item :
        SnowflakeMetadataConnectorProperties.values()) {
      String name = item.name();
      ConnectorProperty property = parseProperty(name);
      builder.add(property);
    }
    return builder.build();
  }

  private static class TaskVariant {

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

  private void addSqlTasksWithInfoSchemaFallback(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull TaskVariant is_task,
      @Nonnull TaskVariant au_task,
      @Nonnull ConnectorArguments arguments) {
    out.addAll(getSqlTasks(header, format, is_task, au_task, arguments));
  }

  private ImmutableList<Task<?>> getSqlTasks(
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull TaskVariant is_task,
      @Nonnull TaskVariant au_task,
      @Nonnull ConnectorArguments arguments) {
    switch (inputSource) {
      case USAGE_THEN_SCHEMA_SOURCE:
        {
          AbstractJdbcTask<Summary> schemaTask = taskFromVariant(format, is_task, header);
          AbstractJdbcTask<Summary> usageTask = taskFromVariant(format, au_task, header);
          if (arguments.isAssessment()) {
            return ImmutableList.of(usageTask);
          }
          return ImmutableList.of(usageTask, schemaTask.onlyIfFailed(usageTask));
        }
      case SCHEMA_ONLY_SOURCE:
        return ImmutableList.of(taskFromVariant(format, is_task, header));
      case USAGE_ONLY_SOURCE:
        return ImmutableList.of(taskFromVariant(format, au_task, header));
    }
    throw new AssertionError();
  }

  private static AbstractJdbcTask<Summary> taskFromVariant(
      String formatString, TaskVariant variant, Class<? extends Enum<?>> header) {
    return new JdbcSelectTask(
            variant.zipEntryName,
            String.format(formatString, variant.schemaName, variant.whereClause))
        .withHeaderClass(header);
  }

  private void addSingleSqlTask(
      @Nonnull List<? super Task<?>> out,
      @Nonnull String format,
      @Nonnull TaskVariant task,
      @Nonnull ResultSetTransformer<String[]> transformer) {
    out.add(
        new JdbcSelectTask(
                task.zipEntryName, String.format(format, task.schemaName, task.whereClause))
            .withHeaderTransformer(transformer));
  }

  private String[] transformHeaderToCamelCase(ResultSet rs, CaseFormat baseFormat)
      throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();
    String[] columns = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = baseFormat.to(CaseFormat.UPPER_CAMEL, metaData.getColumnLabel(i + 1));
    }
    return columns;
  }

  @Override
  public final void addTasksTo(
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
    addSqlTasksWithInfoSchemaFallback(
        out,
        Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT database_name, database_owner FROM %1$s.DATABASES%2$s",
            SnowflakeMetadataConnectorProperties.DATABASES_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.DATABASES_OVERRIDE_WHERE.name()),
        new TaskVariant(DatabasesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(DatabasesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasksWithInfoSchemaFallback(
        out,
        SchemataFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
            SnowflakeMetadataConnectorProperties.SCHEMATA_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.SCHEMATA_OVERRIDE_WHERE.name()),
        new TaskVariant(SchemataFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SchemataFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasksWithInfoSchemaFallback(
        out,
        TablesFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
                + " clustering_key FROM %1$s.TABLES%2$s",
            SnowflakeMetadataConnectorProperties.TABLES_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.TABLES_OVERRIDE_WHERE.name()),
        new TaskVariant(TablesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(TablesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments); // Painfully slow.

    addSqlTasksWithInfoSchemaFallback(
        out,
        ColumnsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name,"
                + " data_type FROM %1$s.COLUMNS%2$s",
            SnowflakeMetadataConnectorProperties.COLUMNS_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.COLUMNS_OVERRIDE_WHERE.name()),
        new TaskVariant(ColumnsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(ColumnsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments); // Very fast.

    addSqlTasksWithInfoSchemaFallback(
        out,
        ViewsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT table_catalog, table_schema, table_name, view_definition FROM %1$s.VIEWS%2$s",
            SnowflakeMetadataConnectorProperties.VIEWS_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.VIEWS_OVERRIDE_WHERE.name()),
        new TaskVariant(ViewsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(ViewsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasksWithInfoSchemaFallback(
        out,
        FunctionsFormat.Header.class,
        getOverrideableQuery(
            arguments,
            "SELECT function_schema, function_name, data_type, argument_signature FROM"
                + " %1$s.FUNCTIONS%2$s",
            SnowflakeMetadataConnectorProperties.FUNCTIONS_OVERRIDE_QUERY.name(),
            SnowflakeMetadataConnectorProperties.FUNCTIONS_OVERRIDE_WHERE.name()),
        new TaskVariant(FunctionsFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(FunctionsFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    if (arguments.isAssessment()) {
      addSingleSqlTask(
          out,
          getOverrideableQuery(
              arguments,
              "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s",
              SnowflakeMetadataConnectorProperties.TABLE_STORAGE_METRICS_OVERRIDE_QUERY.name(),
              SnowflakeMetadataConnectorProperties.TABLE_STORAGE_METRICS_OVERRIDE_WHERE.name()),
          new TaskVariant(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME, AU),
          rs -> transformHeaderToCamelCase(rs, CaseFormat.UPPER_UNDERSCORE));

      ResultSetTransformer<String[]> lowerUnderscoreTransformer =
          rs -> transformHeaderToCamelCase(rs, CaseFormat.LOWER_UNDERSCORE);
      addSingleSqlTask(
          out,
          "SHOW WAREHOUSES",
          new TaskVariant(WarehousesFormat.AU_ZIP_ENTRY_NAME, AU),
          lowerUnderscoreTransformer);
      addSingleSqlTask(
          out,
          "SHOW EXTERNAL TABLES",
          new TaskVariant(ExternalTablesFormat.AU_ZIP_ENTRY_NAME, AU),
          lowerUnderscoreTransformer);
      addSingleSqlTask(
          out,
          "SHOW FUNCTIONS",
          new TaskVariant(FunctionInfoFormat.AU_ZIP_ENTRY_NAME, AU),
          lowerUnderscoreTransformer);
    }
  }

  private String getOverrideableQuery(
      @Nonnull ConnectorArguments arguments,
      @Nonnull String defaultSql,
      @Nonnull String queryProperty,
      @Nonnull String whereProperty) {

    String overrideQuery = arguments.getDefinition(parseProperty(queryProperty));
    if (overrideQuery != null) {
      return overrideQuery;
    }

    String overrideWhere = arguments.getDefinition(parseProperty(whereProperty));
    if (overrideWhere != null) {
      return defaultSql + " WHERE " + overrideWhere;
    }

    return defaultSql;
  }
}
