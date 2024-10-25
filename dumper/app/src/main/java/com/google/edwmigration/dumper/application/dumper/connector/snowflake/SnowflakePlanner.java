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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.ExternalTablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.FunctionInfoFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.TableStorageMetricsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.WarehousesFormat;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
final class SnowflakePlanner {

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

  ImmutableList<AssessmentQuery> generateAssessmentQueries() {
    return ImmutableList.of(
        AssessmentQuery.createMetricsSelect(Format.TABLE_STORAGE_METRICS, UPPER_UNDERSCORE),
        AssessmentQuery.createShow("WAREHOUSES", Format.WAREHOUSES, LOWER_UNDERSCORE),
        AssessmentQuery.createShow("EXTERNAL TABLES", Format.EXTERNAL_TABLES, LOWER_UNDERSCORE),
        AssessmentQuery.createShow("FUNCTIONS", Format.FUNCTION_INFO, LOWER_UNDERSCORE));
  }

  static class AssessmentQuery {
    final String formatString;
    final String zipEntryName;
    private final CaseFormat caseFormat;
    @Nullable private final MetadataView view;

    private AssessmentQuery(
        String formatString,
        String zipEntryName,
        CaseFormat caseFormat,
        @Nullable MetadataView view) {
      this.formatString = formatString;
      this.zipEntryName = zipEntryName;
      this.caseFormat = caseFormat;
      this.view = view;
    }

    static AssessmentQuery createMetricsSelect(Format zipFormat, CaseFormat caseFormat) {
      String formatString = "SELECT * FROM %1$s.TABLE_STORAGE_METRICS%2$s";
      String zipEntryName = zipFormat.value;
      return new AssessmentQuery(
          formatString, zipEntryName, caseFormat, MetadataView.TABLE_STORAGE_METRICS);
    }

    static AssessmentQuery createShow(String view, Format zipFormat, CaseFormat caseFormat) {
      String queryString = String.format("SHOW %s", view);
      return new AssessmentQuery(queryString, zipFormat.value, caseFormat, null);
    }

    Optional<MetadataView> getView() {
      return Optional.ofNullable(view);
    }

    ResultSetTransformer<String[]> transformer() {
      return HeaderTransformers.toCamelCaseFrom(caseFormat);
    }
  }
}
