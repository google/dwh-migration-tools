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
  LOGS,
  METADATA,
  STATS;

  String toDisplayName() {
    if (this == METADATA) {
      return "oracle";
    } else {
      return "oracle-" + getResultType();
    }
  }

  String toFileName(boolean isAssessment) {
    String timeSuffix;
    if (this == LOGS && isAssessment) {
      timeSuffix = LogsConnector.getTimeSuffix();
    } else {
      timeSuffix = "";
    }
    return String.format("dwh-migration-oracle-%s%s.zip", getResultType(), timeSuffix);
  }

  String toFormat() {
    switch (this) {
      case LOGS:
        return OracleLogsDumpFormat.FORMAT_NAME;
      case METADATA:
        return OracleMetadataDumpFormat.FORMAT_NAME;
      case STATS:
        return "oracle.stats.zip";
    }
    throw new AssertionError();
  }

  private String getResultType() {
    switch (this) {
      case LOGS:
        return "logs";
      case METADATA:
        return "metadata";
      case STATS:
        return "stats";
    }
    throw new AssertionError();
  }
}
