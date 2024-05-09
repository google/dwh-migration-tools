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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.OracleLogsDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.OracleMetadataDumpFormat;

enum OracleConnectorScope {
  LOGS("oracle-logs", OracleLogsDumpFormat.FORMAT_NAME, "logs"),
  METADATA("oracle", OracleMetadataDumpFormat.FORMAT_NAME, "metadata"),
  STATS("oracle-stats", "oracle.stats.zip", "stats");

  private final String displayName;
  private final String format;
  private final String resultType;

  OracleConnectorScope(String displayName, String format, String resultType) {
    this.displayName = displayName;
    this.format = format;
    this.resultType = resultType;
  }

  String toDisplayName() {
    return displayName;
  }

  String toFileName(boolean isAssessment) {
    String timeSuffix;
    if (this == LOGS && isAssessment) {
      timeSuffix = LogsConnector.getTimeSuffix();
    } else {
      timeSuffix = "";
    }
    return String.format("dwh-migration-oracle-%s%s.zip", resultType, timeSuffix);
  }

  String toFormat() {
    return format;
  }
}
