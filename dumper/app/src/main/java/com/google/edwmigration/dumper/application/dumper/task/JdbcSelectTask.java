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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.utils.QueryLogDateUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

/** @author shevek */
public class JdbcSelectTask extends AbstractJdbcTask<Summary> {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSelectTask.class);

  @Nonnull private final String sql;

  @Nonnull private final TaskCategory taskCategory;

  private ZonedDateTime logQueryStarDate, logQueryEndDate;

  public JdbcSelectTask(@Nonnull String targetPath, @Nonnull String sql) {
    this(targetPath, sql, TaskCategory.REQUIRED);
  }

  public JdbcSelectTask(
      @Nonnull String targetPath, @Nonnull String sql, TaskCategory taskCategory) {
    super(targetPath);
    this.sql = sql;
    this.taskCategory = taskCategory;
  }

  public JdbcSelectTask(
      @Nonnull String targetPath,
      @Nonnull String sql,
      TaskCategory taskCategory,
      ZonedDateTime logQueryStartDate,
      ZonedDateTime logQueryEndDate) {
    this(targetPath, sql, taskCategory);
    this.logQueryStarDate = logQueryStartDate;
    this.logQueryEndDate = logQueryEndDate;
  }

  @Override
  @Nonnull
  public TaskCategory getCategory() {
    return taskCategory;
  }

  @Nonnull
  public String getSql() {
    return sql;
  }

  @Override
  protected Summary doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink);
    Summary summary = doSelect(connection, rse, sql);
    updateQueryLogDates(summary);
    return summary;
  }

  @Override
  public String describeSourceData() {
    return createSourceDataDescriptionForQuery(getSql());
  }

  private void updateQueryLogDates(Summary summary) {
    if (summary.rowCount() > 0) {
      QueryLogDateUtil.updateQueryLogStartDate(logQueryStarDate);
      QueryLogDateUtil.updateQueryLogEndDate(logQueryEndDate);
    }
  }
}
