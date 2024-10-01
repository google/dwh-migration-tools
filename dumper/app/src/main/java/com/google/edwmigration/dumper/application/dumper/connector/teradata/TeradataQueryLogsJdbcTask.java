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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TeradataQueryLogsJdbcTask extends AbstractJdbcTask<TeradataQueryLogResults> {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataQueryLogsJdbcTask.class);
  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  private String queryLogsTableName;
  private ZonedDateTime queryLogStartDate, queryLogEndDate;

  public TeradataQueryLogsJdbcTask(
      ZonedDateTime queryLogStartDate, ZonedDateTime queryLogEndDate, String queryLogTableNames) {
    super(
        TeradataQueryLogsJdbcTask.class.getSimpleName() + ".txt",
        TargetInitialization.DO_NOT_CREATE);
    Preconditions.checkNotNull(queryLogTableNames, "Query log table names are null");
    this.queryLogsTableName = queryLogTableNames;
    this.queryLogStartDate = queryLogStartDate;
    this.queryLogEndDate = queryLogEndDate;
  }

  @Override
  protected TeradataQueryLogResults doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {

    LOG.info("Getting first and last entry of query logs from '%s", queryLogsTableName);

    String sql =
        String.format(
            "SELECT MIN(StartTime) as queryLogFirstEntry, MAX(StartTime) as queryLogLastEntry FROM '%s' WHERE ErrorCode = 0 AND\n"
                + "StartTime >= CAST('%s' AS TIMESTAMP) AND L.StartTime < CAST('%s' AS TIMESTAMP)\n",
            queryLogsTableName,
            SQL_FORMAT.format(queryLogStartDate),
            SQL_FORMAT.format(queryLogEndDate));

    return null;
  }
}

/** TeradataQueryLogResults */
class TeradataQueryLogResults {
  ZonedDateTime queryLogFirstEntry;
  ZonedDateTime queryLogLastEntry;
}
