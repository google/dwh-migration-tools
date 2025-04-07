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
package com.google.edwmigration.validation.application.validator;

public class NameManager {

  ValidationArguments args;

  public NameManager(ValidationArguments args) {
    this.args = args;
  }

  public enum ValidationType {
    AGGREGATE,
    ROW
  }

  public String getBqSourceTableName(ValidationType type) {
    StringBuilder sb = new StringBuilder();

    String database = args.getSourceConnection().getDatabase();
    if (database != null) {
      sb.append(database).append("_");
    }
    sb.append(args.getTableMapping().getSourceTable().getTable()).append("_");

    if (type == ValidationType.AGGREGATE) {
      sb.append("agg_source");
    } else if (type == ValidationType.ROW) {
      sb.append("row_source");
    }
    return sb.toString();
  }

  public String getBqTargetTableName(ValidationType type) {
    StringBuilder sb = new StringBuilder();

    String database = args.getTargetConnection().getDatabase();
    if (database != null) {
      sb.append(database).append("_");
    }
    sb.append(args.getTableMapping().getTargetTable().getTable()).append("_");
    if (type == ValidationType.AGGREGATE) {
      sb.append("agg_target");
    } else if (type == ValidationType.ROW) {
      sb.append("row_target");
    }
    return sb.toString();
  }
}
