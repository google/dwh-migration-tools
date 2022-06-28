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
package com.google.edwmigration.dumper.sql;

import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class for reading .sql files. */
public final class SqlUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtil.class);

  private SqlUtil() {}

  /**
   * @param sqlPath Path to an .sql file.
   * @return File contents, never null.
   */
  public static String getSql(String sqlPath) throws IOException {
    return Resources.toString(Resources.getResource(sqlPath), UTF_8);
  }

  /**
   * @param connection DB connection parameter
   * @param queries List of strings each of the contains a parametrized SQL request
   */
  public static void executeQueries(Connection connection, List<String> queries)
      throws SQLException {
    for (String query : queries) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.execute();
      } catch (SQLException e) {
        LOGGER.error(format("Cannot execute query: %n%s%n", query));
        throw e;
      }
    }
  }

  /**
   * @param username DB username
   * @param password DB password
   * @param query A single string of a parametrized SQL request
   */
  public static void connectAndExecuteQueryAsUser(String username, String password, String query)
      throws SQLException {
    Connection connection = DriverManager.getConnection(URL_DB, username, password);
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.execute();
    } catch (SQLException e) {
      LOGGER.error(format("Cannot execute query: %n%s%n", query));
      throw e;
    }
  }
}
