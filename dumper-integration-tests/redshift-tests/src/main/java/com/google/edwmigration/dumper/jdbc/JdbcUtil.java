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

import static com.google.edwmigration.dumper.base.TestConstants.TRAILING_SPACES_REGEX;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

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
    return rs.wasNull() ? "" : TRAILING_SPACES_REGEX.matcher(string).replaceFirst("");
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return int or 0 if null.
   */
  public static int getIntNotNull(ResultSet rs, String column) throws SQLException {
    int intValue = rs.getInt(column);
    return rs.wasNull() ? 0 : intValue;
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return long or 0L if null.
   */
  public static long getLongNotNull(ResultSet rs, String column) throws SQLException {
    long longValue = rs.getLong(column);
    return rs.wasNull() ? 0L : longValue;
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return byte[] or empty byte[] if null.
   */
  public static byte[] getBytesNotNull(ResultSet rs, String column) throws SQLException {
    try {
      byte[] bytesValue = rs.getBytes(column);
      return rs.wasNull() ? new byte[0] : bytesValue;
    } catch (SQLException e) {
      BigDecimal bigDecimal = rs.getBigDecimal(column);
      return rs.wasNull() ? new byte[0] : bigDecimal.toBigInteger().toByteArray();
    }
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return double or 0.0 if null.
   */
  public static double getDoubleNotNull(ResultSet rs, String column) throws SQLException {
    double doubleValue = rs.getDouble(column);
    return rs.wasNull() ? 0.0 : doubleValue;
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return long or 0L if null.
   */
  public static long getTimestampNotNull(ResultSet rs, String column) throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Timestamp timestamp = rs.getTimestamp(column, cal);
    if (rs.wasNull()) {
      return 0L;
    }
    return Timestamp.from(
            ZonedDateTime.of(timestamp.toLocalDateTime(), cal.getTimeZone().toZoneId()).toInstant())
        .getTime();
  }

  /**
   * @param rs A row with SELECT results.
   * @param column Database column name.
   * @return boolean or false if null.
   */
  public static boolean getBooleanNotNull(ResultSet rs, String column) throws SQLException {
    boolean bool = rs.getBoolean(column);
    return rs.wasNull() ? false : bool;
  }
}
