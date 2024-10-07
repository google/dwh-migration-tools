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
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

public class TeradataQueryLogsJdbcTask extends AbstractJdbcTask<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataQueryLogsJdbcTask.class);
  private ZonedDateTime queryLogStartDate, queryLogEndDate;
  private final String tableName;
  private final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

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

  @CheckForNull
  @Override
  protected Void doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    LOG.info("Getting first and last query log entries");

    String[] queryLogsFirstAndLastEntries = new String[2];
    String sql = sqlForQueryLogDates(tableName, queryLogStartDate, queryLogEndDate);
    doSelect(connection, newCsvResultSetExtractor(sink, queryLogsFirstAndLastEntries), sql);

    LOG.info(
        "The first query log entry is {} UTC and the last query log entry is {} UTC",
        queryLogsFirstAndLastEntries[0],
        queryLogsFirstAndLastEntries[1]);

    return null;
  }

  private ResultSetExtractor<Void> newCsvResultSetExtractor(
      @Nonnull ByteSink sink, String[] queryLogsFirstAndLastEntries) {
    return rs -> {
      try {
        printAllResults(sink, rs, queryLogsFirstAndLastEntries);
        return null;
      } catch (IOException e) {
        throw new SQLException(e);
      }
    };
  }

  private void printAllResults(
      ByteSink sink, ResultSet resultSet, String[] queryLogsFirstAndLastEntries)
      throws IOException, SQLException {
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream()) {
      int columnCount = resultSet.getMetaData().getColumnCount();
      int index = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          Object resultItem = resultSet.getObject(i);
          String csvItemCandidate = fromByteBufferOrClob(resultItem);
          String itemString;
          if (csvItemCandidate != null || resultItem == null) {
            // Item was recognized by the helper method or it was null.
            LOG.info(csvItemCandidate);
          } else if ((itemString = resultItem.toString()) == null) {
            // Item violated usual toStringRules
            Class<?> itemClass = resultItem.getClass();
            LOG.warn("Unexpected toString result for class {} - null", itemClass);
          } else {
            queryLogsFirstAndLastEntries[index++] = itemString;
          }
        }
      }
    }
  }

  @Nullable
  private static String fromByteBufferOrClob(Object object) throws IOException, SQLException {
    if (object instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) object);
    } else if (object instanceof Clob) {
      InputStream in = ((Clob) object).getAsciiStream();
      StringWriter w = new StringWriter();
      IOUtils.copy(in, w);
      return w.toString();
    } else {
      return null;
    }
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
