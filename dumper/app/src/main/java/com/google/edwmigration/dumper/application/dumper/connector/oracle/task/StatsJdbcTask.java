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
package com.google.edwmigration.dumper.application.dumper.connector.oracle.task;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup.StatsSource.NATIVE;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleStatsQuery;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.Task.StateCondition;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ResultSetExtractor;

@ParametersAreNonnullByDefault
public class StatsJdbcTask extends AbstractJdbcTask<Summary> {

  private static final Logger LOG = LoggerFactory.getLogger(StatsJdbcTask.class);
  private final Condition condition;
  private final OracleStatsQuery query;

  private StatsJdbcTask(OracleStatsQuery query, Condition condition) {
    super(query.name() + ".csv");
    this.condition = condition;
    this.query = query;
  }

  @Nonnull
  public static StatsJdbcTask fromQuery(OracleStatsQuery query) {
    return new StatsJdbcTask(query, Condition.alwaysTrue());
  }

  @CheckReturnValue
  @Nonnull
  @Override
  public StatsJdbcTask onlyIfFailed(Task<?> prerequisite) {
    StateCondition failureCondition = new StateCondition(prerequisite, TaskState.FAILED);
    return new StatsJdbcTask(query, failureCondition);
  }

  @Nonnull
  public static StatsJdbcTask onlyIfAllSkipped(
      OracleStatsQuery query, List<Task<?>> skippableTasks) {
    ImmutableList<Condition> conditions =
        skippableTasks.stream()
            .map(item -> new StateCondition(item, TaskState.SUCCEEDED))
            .collect(toImmutableList());
    Condition skippingCondition = new AndCondition(conditions);
    return new StatsJdbcTask(query, skippingCondition);
  }

  @Deprecated // use onlyIfFailed
  @Override
  @Nonnull
  public AbstractTask<Summary> withCondition(Condition condition) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Condition[] getConditions() {
    return new Condition[] {condition};
  }

  @Nonnull
  @Override
  public Summary doInConnection(
      TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
      throws SQLException {
    ResultSetExtractor<Summary> extractor = newCsvResultSetExtractor(sink);
    Summary result;
    if (query.queryGroup().statsSource() == NATIVE) {
      result = doSelect(connection, extractor, query.queryText());
    } else {
      long days = query.queriedDuration().toDays();
      result = doSelect(connection, extractor, query.queryText(), days);
    }
    if (result == null) {
      LOG.warn("Unexpected data extraction result: null");
      return Summary.EMPTY;
    }
    return result;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    if (query.queryGroup().required()) {
      return TaskCategory.REQUIRED;
    } else {
      return TaskCategory.OPTIONAL;
    }
  }

  @Nonnull
  public static ImmutableList<StatsJdbcTask> findByGroup(
      List<StatsJdbcTask> tasks, QueryGroup group) {
    return tasks.stream()
        .filter(item -> item.query().queryGroup().equals(group))
        .collect(toImmutableList());
  }

  @Nonnull
  OracleStatsQuery query() {
    return query;
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
