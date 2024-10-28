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
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakePlanner.AssessmentQuery;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;

@AutoService(Connector.class)
public final class SnowflakeLiteConnector extends AbstractSnowflakeConnector
    implements SnowflakeMetadataDumpFormat {

  private static final String NAME = "snowflake-lite";

  private final SnowflakeInput inputSource = USAGE_THEN_SCHEMA_SOURCE;
  private final SnowflakePlanner planner = new SnowflakePlanner();

  public SnowflakeLiteConnector() {
    super(NAME);
  }

  @Override
  @Nonnull
  public String getDefaultFileName(boolean isAssessment, Clock clock) {
    return ArchiveNameUtil.getFileName(NAME);
  }

  private void addSqlTasksWithInfoSchemaFallback(
      @Nonnull List<? super Task<?>> out,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String format,
      @Nonnull TaskVariant is_task,
      @Nonnull TaskVariant au_task) {
    ImmutableList<Task<?>> tasks = getSqlTasks(inputSource, header, format, is_task, au_task);
    out.addAll(tasks);
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
        TaskVariant.createWithNoFilter(DatabasesFormat.IS_ZIP_ENTRY_NAME, IS),
        TaskVariant.createWithFilter(DatabasesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE));

    addSqlTasksWithInfoSchemaFallback(
        out,
        SchemataFormat.Header.class,
        "SELECT catalog_name, schema_name FROM %1$s.SCHEMATA%2$s",
        TaskVariant.createWithNoFilter(SchemataFormat.IS_ZIP_ENTRY_NAME, IS),
        TaskVariant.createWithFilter(SchemataFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE));

    addSqlTasksWithInfoSchemaFallback(
        out,
        TablesFormat.Header.class,
        "SELECT table_catalog, table_schema, table_name, table_type, row_count, bytes,"
            + " clustering_key FROM %1$s.TABLES%2$s",
        TaskVariant.createWithNoFilter(TablesFormat.IS_ZIP_ENTRY_NAME, IS),
        TaskVariant.createWithFilter(TablesFormat.AU_ZIP_ENTRY_NAME, AU, AU_WHERE));

    if (arguments.isAssessment()) {
      for (AssessmentQuery item : planner.generateAssessmentQueries()) {
        String query = String.format(item.formatString, AU, /* an empty WHERE clause */ "");
        String zipName = item.zipEntryName;
        Task<?> task = new JdbcSelectTask(zipName, query).withHeaderTransformer(item.transformer());
        out.add(task);
      }
    }
  }
}
