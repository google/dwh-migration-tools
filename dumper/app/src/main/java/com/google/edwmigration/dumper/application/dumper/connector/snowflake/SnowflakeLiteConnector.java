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
import com.google.edwmigration.dumper.application.dumper.connector.ResultSetTransformer;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService(Connector.class)
public final class SnowflakeLiteConnector extends AbstractSnowflakeConnector
    implements SnowflakeMetadataDumpFormat {

  private static final String NAME = "snowflake-lite";

  private final SnowflakeInput inputSource = USAGE_THEN_SCHEMA_SOURCE;

  public SnowflakeLiteConnector() {
    super(NAME);
  }

  @Override
  @Nonnull
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileName(NAME);
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

    addSqlTasksWithInfoSchemaFallback(
        out,
        DatabasesFormat.Header.class,
        "SELECT database_name, database_owner FROM %1$s.DATABASES%2$s",
        new TaskVariant(DatabasesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(DatabasesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasksWithInfoSchemaFallback(
        out,
        SchemataFormat.Header.class,
        "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
        new TaskVariant(SchemataFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(SchemataFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments);

    addSqlTasksWithInfoSchemaFallback(
        out,
        TablesFormat.Header.class,
        "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
            + " clustering_key FROM %1$s.TABLES%2$s",
        new TaskVariant(TablesFormat.IS_ZIP_ENTRY_NAME, IS),
        new TaskVariant(TablesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE),
        arguments); // Painfully slow.

    if (arguments.isAssessment()) {
      addSingleSqlTask(
          out,
          "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s",
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
}
