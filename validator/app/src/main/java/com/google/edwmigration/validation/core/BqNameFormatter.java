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
package com.google.edwmigration.validation.core;

import com.google.edwmigration.validation.config.ValidationConfig;

public class BqNameFormatter {

  private final ValidationConfig config;

  public enum ValidationType {
    AGGREGATE,
    ROW
  }

  public BqNameFormatter(ValidationConfig config) {
    this.config = config;
  }

  // TODO delete me
  public String getDataset() {
    return config.BqTargetTable.dataset;
  }

  public String getFullyQualifiedBqSourceTableName(ValidationType type) {
    return getDataset() + "." + getBqSourceTableName(type);
  }

  public String getFullyQualifiedBqTargetTableName(ValidationType type) {
    return getDataset() + "." + getBqTargetTableName(type);
  }

  // bq "source" table doesn't make sense...
  public String getBqSourceTableName(ValidationType type) {
    StringBuilder sb = new StringBuilder();

    String database = config.SourceConnection.database;
    if (database != null) {
      sb.append(database).append("_");
    }
    sb.append(config.SourceTable + "_");

    sb.append(type == ValidationType.AGGREGATE ? "agg_source" : "row_source");
    return sb.toString();
  }

  public String getBqTargetTableName(ValidationType type) {
    StringBuilder sb = new StringBuilder();

    String database = config.BqTargetTable.dataset;
    // getTargetConnection().getDatabase();
    if (database != null) {
      sb.append(database).append("_");
    }
    sb.append(config.SourceTable + "_"); // target? why source?

    sb.append(type == ValidationType.AGGREGATE ? "agg_target" : "row_target");
    return sb.toString();
  }
}
