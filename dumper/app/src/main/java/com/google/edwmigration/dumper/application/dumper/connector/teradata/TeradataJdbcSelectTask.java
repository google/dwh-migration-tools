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
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SingleColumnRowMapper;

/** Task for selecting rows in Teradata connectors. */
public class TeradataJdbcSelectTask extends JdbcSelectTask {
  private static final Logger LOG = LoggerFactory.getLogger(TeradataJdbcSelectTask.class);

  private final String sqlCount;
  private final TaskCategory category;

  public TeradataJdbcSelectTask(String targetPath, TaskCategory category, String sqlTemplate) {
    super(targetPath, String.format(sqlTemplate, "*"));
    this.sqlCount = sqlTemplate.contains("%s") ? String.format(sqlTemplate, "count(*)") : null;
    this.category = Preconditions.checkNotNull(category);
  }

  @Override
  public TaskCategory getCategory() {
    return category;
  }

  // This works to execute the count SQL on the same connection as the data SQL,
  // because it's called from doInConnection, when we already have one connection open.
  @Nonnull
  private ResultSetExtractor<Void> newCountedResultSetExtractor(
      @Nonnull ByteSink sink, @Nonnull Connection connection) throws SQLException {
    long count = -1;
    if (sqlCount != null) {
      // It's a lot of infrastructure, but we don't have to write it.
      RowMapper<Long> rowMapper = new SingleColumnRowMapper<>(Long.class);
      ResultSetExtractor<List<Long>> resultSetExtractor =
          new RowMapperResultSetExtractor<>(rowMapper);
      List<Long> results = doSelect(connection, resultSetExtractor, sqlCount);
      Long result = DataAccessUtils.nullableSingleResult(results);
      if (result != null) count = result;
    }
    return newCsvResultSetExtractor(sink, count);
  }

  @Override
  protected Void doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    try {
      connection.setAutoCommit(false);
      PreparedStatement statement =
          connection.prepareStatement(
              "SET QUERY_BAND='ApplicationName=compilerworks;' FOR SESSION");
      statement.execute();
    } catch (SQLException e) {
      // We might still be in postgresql or sqlite.
      LOG.warn("Failed to set QUERY_BAND: " + e);
      // This puts the transaction in an aborted state unless we rollback here.
      connection.rollback();
    }
    ResultSetExtractor<Void> rse = newCountedResultSetExtractor(sink, connection);
    return doSelect(connection, rse, getSql());
  }
}
