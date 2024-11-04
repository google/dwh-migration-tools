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

import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
final class SnowflakeTaskUtil {

  static AbstractJdbcTask<Summary> withFilter(
      String format,
      String schemaName,
      String zipEntryName,
      String whereClause,
      Class<? extends Enum<?>> header) {
    String sql = String.format(format, schemaName, whereClause);
    return new JdbcSelectTask(zipEntryName, sql).withHeaderClass(header);
  }

  static AbstractJdbcTask<Summary> withNoFilter(
      String format, String schemaName, String zipEntryName, Class<? extends Enum<?>> header) {
    // required, because the format string parameter takes two args
    String whereClause = "";
    String sql = String.format(format, schemaName, whereClause);
    return new JdbcSelectTask(zipEntryName, sql).withHeaderClass(header);
  }

  private SnowflakeTaskUtil() {}
}
