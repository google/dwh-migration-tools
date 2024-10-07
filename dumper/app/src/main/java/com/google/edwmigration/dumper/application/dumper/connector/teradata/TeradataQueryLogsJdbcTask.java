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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.annotation.ParametersAreNonnullByDefault;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

@ParametersAreNonnullByDefault
public class TeradataQueryLogsJdbcTask extends AbstractJdbcTask<QueryLogEntries> {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataQueryLogsJdbcTask.class);
  private final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
  private ZonedDateTime queryLogStartDate, queryLogEndDate;
  private final String tableName;

  public TeradataQueryLogsJdbcTask(
      String targetPath,
      String tableName,
      ZonedDateTime queryLogStartDate,
      ZonedDateTime queryLogEndDate) {
    super(targetPath);
    this.tableName = tableName;
    this.queryLogStartDate = queryLogStartDate;
    this.queryLogEndDate = queryLogEndDate;
  }

  @Override
  protected QueryLogEntries doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    LOG.info("Getting first and last query log entries");
    String sql = sqlForQueryLogDates(tableName, queryLogStartDate, queryLogEndDate);
    QueryLogEntries queryLogEntries = doSelect(connection, csvResultSetExtractor(sink), sql);

    /* If the values are null, means that there is no query log dates in db,
        so we dont output any message
    */
    if (queryLogEntries.queryLogFirstEntry != null && queryLogEntries.queryLogLastEntry != null) {
      LOG.info(
          "The first query log entry is {} UTC and the last query log entry is {} UTC",
          queryLogEntries.queryLogFirstEntry,
          queryLogEntries.queryLogLastEntry);
    }
    return queryLogEntries;
  }

  private ResultSetExtractor<QueryLogEntries> csvResultSetExtractor(ByteSink sink) {
    return rs -> {
      try {
        return getFirstAndLastQueryLogEntries(sink, rs);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }

  private QueryLogEntries getFirstAndLastQueryLogEntries(ByteSink sink, ResultSet resultSet)
      throws IOException, SQLException {
    QueryLogEntries queryLogEntries = new QueryLogEntries();
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream()) {
      /*
       * We query only min and max, so returning set should contain only 1 row and 2 values
       */
      if (resultSet.next()) {
        Object firstEntryObject = resultSet.getObject(1);
        Object lastEntryObject = resultSet.getObject(2);

        if (firstEntryObject != null) {
          queryLogEntries.queryLogFirstEntry = firstEntryObject.toString();
        }
        if (lastEntryObject != null) {
          queryLogEntries.queryLogLastEntry = lastEntryObject.toString();
        }
      }
    }
    return queryLogEntries;
  }

  private String sqlForQueryLogDates(
      String queryLogsTableName, ZonedDateTime queryLogStartDate, ZonedDateTime queryLogEndDate) {
    String sql =
        String.format(
            "SELECT MIN(StartTime), MAX(StartTime) FROM %s WHERE ErrorCode = 0"
                + " AND StartTime >= CAST('%s' AS TIMESTAMP) AND StartTime < CAST('%s' AS TIMESTAMP)",
            queryLogsTableName,
            SQL_FORMAT.format(queryLogStartDate),
            SQL_FORMAT.format(queryLogEndDate));
    return sql;
  }
}

class QueryLogEntries {
  String queryLogFirstEntry;
  String queryLogLastEntry;
}
