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
package com.google.edwmigration.validation.connector.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.connector.api.Handle;
import com.google.edwmigration.validation.io.writer.ResultSetWriter;
import com.google.edwmigration.validation.io.writer.ResultSetWriterFactory;
import com.google.edwmigration.validation.model.ExecutionState;
import com.google.edwmigration.validation.model.UserInputContext;
import com.google.edwmigration.validation.sql.AbstractSqlGenerator;
import java.net.URI;
import java.sql.*;
import java.util.HashMap;
import javax.annotation.Nonnull;
import org.jooq.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.JdbcUtils;

public abstract class AbstractSourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSourceTask.class);

  private final Handle handle;
  private final URI outputUri;
  protected final UserInputContext context;

  private ResultSetMetaData aggregateQueryMetadata;
  private ResultSetMetaData rowQueryMetadata;

  private final ResultSetWriterFactory writerFactory;

  public AbstractSourceTask(ExecutionState state, ResultSetWriterFactory writerFactory) {
    Preconditions.checkNotNull(state.sourceHandle, "ExecutionState `sourceHandle` is null.");
    this.handle = state.sourceHandle;
    this.outputUri = state.outputUri;
    this.context = state.context;
    this.writerFactory = writerFactory;
  }

  public Handle getHandle() {
    return handle;
  }

  public URI getOutputUri() {
    return outputUri;
  }

  public UserInputContext getContext() {
    return context;
  }

  public void setAggregateQueryMetadata(ResultSetMetaData metadata) {
    this.aggregateQueryMetadata = metadata;
  }

  public ResultSetMetaData getAggregateQueryMetadata() {
    return this.aggregateQueryMetadata;
  }

  public void setRowQueryMetadata(ResultSetMetaData metadata) {
    this.rowQueryMetadata = metadata;
  }

  public ResultSetMetaData getRowQueryMetadata() {
    return this.rowQueryMetadata;
  }

  public abstract void run() throws Exception;

  public String describeSourceData() {
    return "from" + getClass().getSimpleName();
  }

  public String toString() {
    return String.format("Write to %s %s", outputUri, describeSourceData());
  }

  protected HashMap<String, DataType<? extends Number>> executeNumericColsQuery(
      Connection connection, AbstractSqlGenerator generator, String sql) throws SQLException {
    PreparedStatement statement = null;
    try {
      LOG.debug("Preparing numeric column statement...");
      connection.setAutoCommit(false);
      statement =
          connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(16384);
      statement.execute();
      ResultSet rs = null;
      try {
        rs = statement.getResultSet();
        HashMap<String, DataType<? extends Number>> results = new HashMap<>();
        while (rs.next()) {
          String columnName = rs.getString(1);
          String dataType = rs.getString(2);
          int precision = rs.getInt(3);
          int scale = rs.getInt(4);
          DataType<? extends Number> sqlDataType =
              generator.getSqlDataType(dataType, precision, scale);
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
      String tableName, Connection connection, String sql, ValidationType type)
      throws SQLException {
    ResultSetWriter writer = writerFactory.create(outputUri, tableName, type);
    return doSelect(connection, writer, sql);
  }

  protected static <T> ResultSetMetaData doSelect(
      @Nonnull Connection connection,
      @Nonnull ResultSetExtractor<T> resultSetExtractor,
      @Nonnull String sql)
      throws SQLException {
    PreparedStatement statement = null;
    try {
      LOG.debug("Preparing query statement...");
      connection.setAutoCommit(false);
      statement =
          connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(16384);
      Stopwatch stopwatch = Stopwatch.createStarted();
      statement.execute();
      LOG.debug("Statement execution took {}. Extracting results...", stopwatch);

      ResultSet rs = null;
      try {
        rs = statement.getResultSet();
        resultSetExtractor.extractData(rs);
        return rs.getMetaData();
      } catch (DataAccessException e) {
        throw new RuntimeException(e);
      } finally {
        JdbcUtils.closeResultSet(rs);
      }
    } finally {
      JdbcUtils.closeStatement(statement);
    }
  }
}
