/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A helper class for checking Null values returned by executing SELECT request against a database.
 */
public final class JdbcUtil {

  private JdbcUtil() {}

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return String or an empty string if null.
   */
  public static String getStringNotNull(ResultSet rs, String column) throws SQLException {
    String string = rs.getString(column);
    return rs.wasNull() ? "" : string;
  }

  /**
   * @param rs A row with SELECT results.
   * @param columnIndex Database column index.
   * @return String or an empty string if null.
   */
  public static String getStringNotNull(ResultSet rs, int columnIndex) throws SQLException {
    String string = rs.getString(columnIndex);
    return rs.wasNull() ? "" : string;
  }

  /**
   * @param rsmd Metadata of the executed SQL query.
   * @return List of column names.
   * @throws SQLException
   */
  public static List<String> getDbColumnNames(ResultSetMetaData rsmd) throws SQLException {
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      columnNames.add(rsmd.getColumnName(i));
    }
    return columnNames;
  }
}
