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

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.ExternalTablesFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.FunctionInfoFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.TableStorageMetricsFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeMetadataDumpFormat.WarehousesFormat;

final class SnowflakePlanner {

  ImmutableList<AssessmentQuery> generateAssessmentQueries(String overrideableQuery) {
    return ImmutableList.of(
        AssessmentQuery.create(
            overrideableQuery,
            TableStorageMetricsFormat.AU_ZIP_ENTRY_NAME,
            CaseFormat.UPPER_UNDERSCORE),
        AssessmentQuery.create(
            "SHOW WAREHOUSES", WarehousesFormat.AU_ZIP_ENTRY_NAME, CaseFormat.LOWER_UNDERSCORE),
        AssessmentQuery.create(
            "SHOW EXTERNAL TABLES",
            ExternalTablesFormat.AU_ZIP_ENTRY_NAME,
            CaseFormat.LOWER_UNDERSCORE),
        AssessmentQuery.create(
            "SHOW FUNCTIONS", FunctionInfoFormat.AU_ZIP_ENTRY_NAME, CaseFormat.LOWER_UNDERSCORE));
  }

  static class AssessmentQuery {
    final String formatString;
    final String zipEntryName;
    final CaseFormat caseFormat;

    private AssessmentQuery(String formatString, String zipEntryName, CaseFormat caseFormat) {
      this.formatString = formatString;
      this.zipEntryName = zipEntryName;
      this.caseFormat = caseFormat;
    }

    private static AssessmentQuery create(
        String formatString, String zipEntryName, CaseFormat caseFormat) {
      return new AssessmentQuery(formatString, zipEntryName, caseFormat);
    }
  }
}
