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
import com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsQuery;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
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

  private final String query;

  private StatsJdbcTask(String name, String query) {
    super(name);
    Preconditions.checkArgument(name.endsWith(".csv"));
    this.query = query;
  }

  @Nonnull
  static Task<?> fromQuery(StatsQuery query) throws IOException {
    String name = "oracle-stats/" + query.name() + ".csv";
    String queryText = query.queryText();
    return new StatsJdbcTask(name, queryText);
  }

  @Nonnull
  @Override
  public Summary doInConnection(
      @Nonnull TaskRunContext context,
      @Nonnull JdbcHandle jdbcHandle,
      @Nonnull ByteSink sink,
      @Nonnull Connection connection)
      throws SQLException {
    ResultSetExtractor<Summary> extractor = newCsvResultSetExtractor(sink);
    Summary result = doSelect(connection, extractor, query);
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

  @Nonnull
  @Override
  public String describeSourceData() {
    return "";
  }

  @Override
  @Nonnull
  protected CSVFormat newCsvFormat(ResultSet resultSet) throws SQLException {
    return FORMAT.builder().setHeader(resultSet).build();
  }
}
