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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.ExternalTablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.FunctionInfoFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.PrimaryKeysFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.TableStorageMetricsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.UniqueKeysFormat;
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
  public static final AssessmentQuery SHOW_PRIMARY_KEYS =
      AssessmentQuery.createShow("PRIMARY KEYS", Format.PRIMARY_KEYS);
  public static final AssessmentQuery SHOW_UNIQUE_KEYS =
      AssessmentQuery.createShow("UNIQUE KEYS", Format.UNIQUE_KEYS);

  private enum Format {
    CORTEX_AI_SEARCH("cortex_ai_search-au.csv"),
    CORTEX_AI_SQL("cortex_ai_sql-au.csv"),
    CORTEX_ANALYST("cortex_analyst-au.csv"),
    CORTEX_FUNCTIONS("cortex_functions-au.csv"),
    CORTEX_QUERY("cortex_query-au.csv"),
    DATA_TRANSFER("data_transfer-au.csv"),
    DOCUMENT_AI("document_ai-au.csv"),
    EXTERNAL_TABLES(ExternalTablesFormat.AU_ZIP_ENTRY_NAME),
    FUNCTION_INFO(FunctionInfoFormat.AU_ZIP_ENTRY_NAME),
    HYBRID_TABLE_USAGE("hybrid_table_usage-au.csv"),
    PRIMARY_KEYS(PrimaryKeysFormat.AU_ZIP_ENTRY_NAME),
    SEARCH_OPTIMIZATION("search_optimization-au.csv"),
    SNOWPIPE_STREAMING("snowpipe_streaming-au.csv"),
    STAGE_STORAGE_USAGE("stage_storage_usage-au.csv"),
    TABLE_STORAGE_METRICS(TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME),
    UNIQUE_KEYS(UniqueKeysFormat.AU_ZIP_ENTRY_NAME),
    USER_DEFINED_FUNCTIONS(UserDefinedFunctionsFormat.IS_ZIP_ENTRY_NAME),
    VIEW_REFRESH("view_refresh-au.csv"),
    WAREHOUSES(WarehousesFormat.AU_ZIP_ENTRY_NAME);

    private final String value;

    Format(String value) {
      this.value = value;
    }
  }

  private final ImmutableList<AssessmentQuery> assessmentQueries =
      ImmutableList.of(
          AssessmentQuery.createCortexAiSearchSelect(),
          AssessmentQuery.createCortexAiSqlSelect(),
          AssessmentQuery.createCortexAnalystSelect(),
          AssessmentQuery.createCortexFunctionsSelect(),
          AssessmentQuery.createCortexQuerySelect(),
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
          SHOW_EXTERNAL_TABLES,
          SHOW_PRIMARY_KEYS,
          SHOW_UNIQUE_KEYS);

  AssessmentQuery externalTablesInDatabase(String quotedDatabaseName) {
    String query = String.format("SHOW EXTERNAL TABLES IN DATABASE %s", quotedDatabaseName);
    String zipEntryName = Format.EXTERNAL_TABLES.value;
    return new AssessmentQuery(false, query, zipEntryName, LOWER_UNDERSCORE);
  }

  AssessmentQuery primaryKeysInDatabase(String quotedDatabaseName) {
    String query = String.format("SHOW PRIMARY KEYS IN DATABASE %s", quotedDatabaseName);
    String zipEntryName = Format.PRIMARY_KEYS.value;
    return new AssessmentQuery(false, query, zipEntryName, LOWER_UNDERSCORE);
  }

  AssessmentQuery uniqueKeysInDatabase(String quotedDatabaseName) {
    String query = String.format("SHOW UNIQUE KEYS IN DATABASE %s", quotedDatabaseName);
    String zipEntryName = Format.UNIQUE_KEYS.value;
    return new AssessmentQuery(false, query, zipEntryName, LOWER_UNDERSCORE);
  }

  ImmutableList<AssessmentQuery> generateAssessmentQueries() {
    return assessmentQueries;
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

    static AssessmentQuery createCortexAnalystSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.CORTEX_ANALYST.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createCortexFunctionsSelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY";
      String startTime = "CURRENT_TIMESTAMP(0) - INTERVAL '30 days'";
      String query = String.format("SELECT * FROM %s WHERE start_time > %s", view, startTime);
      return new AssessmentQuery(false, query, Format.CORTEX_FUNCTIONS.value, UPPER_UNDERSCORE);
    }

    static AssessmentQuery createCortexQuerySelect() {
      String view = "SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY";
      String query = String.format("SELECT * FROM %s", view);
      return new AssessmentQuery(false, query, Format.CORTEX_QUERY.value, UPPER_UNDERSCORE);
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
          "SELECT HASH(FUNCTION_CATALOG, FUNCTION_SCHEMA, FUNCTION_NAME, ARGUMENT_SIGNATURE) AS FUNCTION_ID, "
              + "FUNCTION_CATALOG, FUNCTION_SCHEMA, FUNCTION_NAME, FUNCTION_LANGUAGE, ARGUMENT_SIGNATURE, "
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
}
