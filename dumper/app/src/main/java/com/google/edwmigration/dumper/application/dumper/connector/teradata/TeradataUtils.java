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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.base.Preconditions;
import java.util.Optional;

class TeradataUtils {

  /**
   * Formats the query by:
   *
   * <ul>
   *   <li>replacing consecutive whitespace characters with a single space,
   *   <li>removing the whitespace immediately following the opening bracket "(" and immediately
   *       preceding the closing bracket ")",
   *   <li>removing the whitespace at the beginning and at the end of the query.
   * </ul>
   *
   * <h3>Remark about string literals</h3>
   *
   * <p>The formatting is applied across the whole query, including string literals. If the input
   * query contains string literals with whitespace, they will be modified, which might be
   * undesirable. This method should not be used in such cases.
   */
  static String formatQuery(String query) {
    return query.replaceAll("\\s+", " ").replaceAll("\\( ", "(").replaceAll(" \\)", ")").trim();
  }

  static String createTimestampExpression(String tableAlias, String columnName) {
    return createTimestampExpression(Optional.of(tableAlias), columnName);
  }

  static String createTimestampExpression(String columnName) {
    return createTimestampExpression(/* tableAlias= */ Optional.empty(), columnName);
  }

  private static String createTimestampExpression(Optional<String> tableAlias, String columnName) {
    Preconditions.checkArgument(!columnName.isEmpty(), "Column name must not be empty.");
    Preconditions.checkArgument(
        tableAlias.map(alias -> !alias.isEmpty()).orElse(true), "Alias must not be empty.");
    StringBuilder buf = new StringBuilder();
    tableAlias.ifPresent(alias -> buf.append(alias).append('.'));
    buf.append(columnName)
        .append(" AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"")
        .append(columnName)
        .append("\"");
    return buf.toString();
  }

  private TeradataUtils() {}
}
