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
package com.google.edwmigration.validation.util;

import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.model.UserInputContext;

public final class BigQueryTableNamer {

  private BigQueryTableNamer() {
    // Utility class; prevent instantiation.
  }

  public static String getFullyQualifiedSourceTableName(
      UserInputContext context, ValidationType type) {
    return context.bqTargetTable.dataset + "." + getSourceTableName(context, type);
  }

  public static String getFullyQualifiedTargetTableName(
      UserInputContext context, ValidationType type) {
    return context.bqTargetTable.dataset + "." + getTargetTableName(context, type);
  }

  // Represents source data uploaded to GCS and queried in BQ as an external table.
  public static String getSourceTableName(UserInputContext context, ValidationType type) {
    return String.format(
        "%s_%s_%s_source",
        context.sourceConnection.database,
        context.sourceTable.name,
        type == ValidationType.AGGREGATE ? "agg" : "row");
  }

  public static String getTargetTableName(UserInputContext context, ValidationType type) {
    return String.format(
        "%s_%s_%s_target",
        context.bqTargetTable.dataset,
        context.bqTargetTable.name,
        type == ValidationType.AGGREGATE ? "agg" : "row");
  }
}
