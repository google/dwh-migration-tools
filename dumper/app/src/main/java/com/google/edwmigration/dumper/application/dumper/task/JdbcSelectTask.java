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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

/** @author shevek */
public class JdbcSelectTask extends AbstractJdbcTask<Summary> {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSelectTask.class);

  @Nonnull private final String sql;
  @Nullable private final ZonedInterval interval;

  public JdbcSelectTask(
      @Nonnull String targetPath, @Nonnull String sql, @Nullable ZonedInterval interval) {
    super(targetPath);
    this.sql = sql;
    this.interval = interval;
  }

  public JdbcSelectTask(@Nonnull String targetPath, @Nonnull String sql) {
    this(targetPath, sql, null);
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
    ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink, -1, Optional.ofNullable(interval));
    return doSelect(connection, rse, sql);
  }

  @Override
  public String toString() {
    // This is called in a situation where we have a variable amount of indentation. See
    // MetadataDumper.
    return "Write " + getTargetPath() + " from:\n        " + getSql();
  }
}
