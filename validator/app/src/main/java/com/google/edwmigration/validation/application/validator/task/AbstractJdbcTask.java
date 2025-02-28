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
package com.google.edwmigration.validation.application.validator.task;

import com.google.common.base.Stopwatch;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.JdbcUtils;

/** @author nehanene */
public abstract class AbstractJdbcTask extends AbstractTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcTask.class);

  public AbstractJdbcTask(Handle handle, URI outputUri, ValidationArguments arguments) {
    super(handle, outputUri, arguments);
  }

  private String createCsvFilePath(QueryType queryType) {
    String tableName = getArguments().getTable();
    String filename = null;

    switch (queryType) {
      case AGGREGATE:
        filename = tableName + "_aggregate.csv";
        break;
      case ROW:
        filename = tableName + "_row_sample.csv";
        break;
    }

    Path filePath = Paths.get(getOutputUri()).resolve(filename);
    return filePath.toString();
  }

  protected void doInConnection(Connection connection, String sql, QueryType type)
      throws SQLException {
    CsvResultSetExtractor rse = new CsvResultSetExtractor(createCsvFilePath(type));
    doSelect(connection, rse, sql);
  }

  @CheckForNull
  protected static <T> T doSelect(
      @Nonnull Connection connection,
      @Nonnull ResultSetExtractor<T> resultSetExtractor,
      @Nonnull String sql)
      throws SQLException {
    PreparedStatement statement = null;
    try {
      LOG.debug("Preparing statement...");

      PREPARE:
      {
        Stopwatch stopwatch = Stopwatch.createStarted();
        // Causes PostgreSQL to use cursors, rather than RAM.
        // https://jdbc.postgresql.org/documentation/83/query.html#fetchsize-example
        // https://medium.com/@FranckPachot/oracle-postgres-jdbc-fetch-size-3012d494712
        connection.setAutoCommit(false);
        // connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);    // Shouldn't be
        // required.
        statement =
            connection.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        // statement.setFetchDirection(ResultSet.FETCH_FORWARD);   // PostgreSQL and (allegedly)
        // Teradata prefer this. However, it is the default, and sqlite throws.
        // Enables cursors in PostgreSQL.
        // Teradata says that this can reduce the fetch size below 1Mb, but not increase it.
        statement.setFetchSize(16384);
        LOG.debug("Statement preparation took {}. Executing...", stopwatch);
      }

      EXECUTE:
      {
        // debug(statement);
        Stopwatch stopwatch = Stopwatch.createStarted();
        statement.execute(); // Must return true to indicate a ResultSet object.
        LOG.debug("Statement execution took {}. Extracting results...", stopwatch);
        // debug(statement);
      }

      T result = null;
      ResultSet rs = null;
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        rs = statement.getResultSet();
        result = resultSetExtractor.extractData(rs);
        LOG.debug("Result set extraction took {}.", stopwatch);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        JdbcUtils.closeResultSet(rs);
      }

      SQLWarning warning = statement.getWarnings();
      while (warning != null) {
        LOG.warn(
            "SQL warning: ["
                + warning.getSQLState()
                + "/"
                + warning.getErrorCode()
                + "] "
                + warning.getMessage());
        warning = warning.getNextWarning();
      }

      return result;
    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }
}
