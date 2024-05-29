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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

@ParametersAreNonnullByDefault
class StatsJdbcTask extends AbstractJdbcTask<Summary> {

  private static final Logger LOG = LoggerFactory.getLogger(StatsJdbcTask.class);

  private final OracleStatsQuery query;

  private StatsJdbcTask(String name, OracleStatsQuery query) {
    super(name);
    Preconditions.checkArgument(name.endsWith(".csv"));
    this.query = query;
  }

  @Nonnull
  static Task<?> fromQuery(OracleStatsQuery query) {
    return new StatsJdbcTask(query.name() + ".csv", query);
  }

  @Nonnull
  @Override
  public Summary doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    ResultSetExtractor<Summary> extractor = newCsvResultSetExtractor(sink);
    Summary result = doSelect(connection, extractor, query.queryText());
    if (result == null) {
      LOG.warn("Unexpected data extraction result: null");
      return Summary.EMPTY;
    }
    return result;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.REQUIRED;
  }

  @Override
  @Nonnull
  protected CSVFormat newCsvFormat(ResultSet resultSet) throws SQLException {
    return FORMAT.builder().setHeader(resultSet).build();
  }

  @Nonnull
  @Override
  public String toString() {
    return String.format("Write to %s: %s", getTargetPath(), query.description());
  }
}
