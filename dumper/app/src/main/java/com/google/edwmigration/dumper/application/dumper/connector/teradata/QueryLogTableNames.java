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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;

/**
 * Table names to extract the query logs from. See the description in {@link
 * QueryLogTableNamesResolver}.
 */
@AutoValue
abstract class QueryLogTableNames {

  /** @return query logs table name */
  abstract String queryLogsTableName();

  /** @return SQL logs table name */
  abstract String sqlLogsTableName();

  /** @return whether at least one alternate table is being used */
  abstract boolean usingAtLeastOneAlternate();

  static QueryLogTableNames create(
      String queryLogsTableName, String sqlLogsTableName, boolean usingAtLeastOneAlternate) {
    Preconditions.checkArgument(
        !queryLogsTableName.isEmpty(), "Query log table name cannot be empty.");
    Preconditions.checkArgument(!sqlLogsTableName.isEmpty(), "SQL log table name cannot be empty.");
    return new AutoValue_QueryLogTableNames(
        queryLogsTableName, sqlLogsTableName, usingAtLeastOneAlternate);
  }
}
