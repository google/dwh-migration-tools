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

  private enum Format {
    EXTERNAL_TABLES(ExternalTablesFormat.ZIP_ENTRY_NAME),
    FUNCTION_INFO(FunctionInfoFormat.AU_ZIP_ENTRY_NAME),
    TABLE_STORAGE_METRICS(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME),
    WAREHOUSES(WarehousesFormat.AU_ZIP_ENTRY_NAME);

    private final String value;

    Format(String value) {
      this.value = value;
    }
  }

  ImmutableList<AssessmentQuery> generateAssessmentQueries() {
    return ImmutableList.of(
        AssessmentQuery.createMetricsSelect(Format.TABLE_STORAGE_METRICS, UPPER_UNDERSCORE),
        AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES, LOWER_UNDERSCORE),
        AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES, LOWER_UNDERSCORE),
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
}
