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
import com.google.edwmigration.validation.application.validator.NameManager.ValidationType;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.sql.AbstractSqlGenerator;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.jooq.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.JdbcUtils;

/** @author nehanene */
public abstract class AbstractJdbcSourceTask extends AbstractSourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcSourceTask.class);

  public AbstractJdbcSourceTask(Handle handle, URI outputUri, ValidationArguments arguments) {
    super(handle, outputUri, arguments);
  }

  private String createCsvFilePath(ValidationType validationType) {
    String tableName = getArguments().getTableMapping().getSourceTable().getTable();
    String filename = null;

    switch (validationType) {
      case AGGREGATE:
        filename = tableName + CSV_AGGREGATE_SUFFIX;
        break;
      case ROW:
        filename = tableName + CSV_ROW_SUFFIX;
        break;
    }

    Path filePath = Paths.get(getOutputUri()).resolve(filename);
    return filePath.toString();
  }

  protected HashMap<String, DataType<? extends Number>> executeNumericColsQuery(
      Connection connection, AbstractSqlGenerator generator, String sql) throws SQLException {
    PreparedStatement statement = null;
    try {
      LOG.debug("Preparing statement...");

      PREPARE:
      {
        Stopwatch stopwatch = Stopwatch.createStarted();
        connection.setAutoCommit(false);
        statement =
            connection.prepareStatement(
                sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(16384);
        LOG.debug("Statement preparation took {}. Executing...", stopwatch);
      }

      EXECUTE:
      {
        statement.execute(); // Must return true to indicate a ResultSet object.
      }

      ResultSet rs = null;
      try {
        rs = statement.getResultSet();
        HashMap<String, DataType<? extends Number>> results = new HashMap<>();

        while (rs.next()) {
          String columnName = rs.getString(1);
          String dataType = rs.getString(2);
          Integer numericPrecision = rs.getInt(3);
          Integer numericScale = rs.getInt(4);
          DataType<? extends Number> sqlDataType =
              generator.getSqlDataType(dataType, numericPrecision, numericScale);
          results.put(columnName, sqlDataType);
        }

        return results;
      } finally {
        JdbcUtils.closeResultSet(rs);
      }

    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }

  protected ResultSetMetaData extractQueryResults(
      Connection connection, String sql, ValidationType type) throws SQLException {
    CsvResultSetExtractor rse = new CsvResultSetExtractor(createCsvFilePath(type));
    return doSelect(connection, rse, sql);
  }

  @CheckForNull
  protected static <T> ResultSetMetaData doSelect(
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

      ResultSet rs = null;
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        rs = statement.getResultSet();
        resultSetExtractor.extractData(rs);
        LOG.debug("Result set extraction took {}.", stopwatch);
        return rs.getMetaData();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        JdbcUtils.closeResultSet(rs);
      }

    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }
}
