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

import static com.google.common.base.CaseFormat.UPPER_CAMEL;

import com.google.common.base.CaseFormat;
import com.google.edwmigration.dumper.application.dumper.connector.ResultSetTransformer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
final class HeaderTransformers {

  static ResultSetTransformer<String[]> toCamelCaseFrom(CaseFormat format) {
    return resultSet -> transformToCamelCase(resultSet, format);
  }

  private static String[] transformToCamelCase(ResultSet rs, CaseFormat baseFormat)
      throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();
    String[] columns = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columns[i] = baseFormat.to(UPPER_CAMEL, metaData.getColumnLabel(i + 1));
    }
    return columns;
  }

  private HeaderTransformers() {}
}
