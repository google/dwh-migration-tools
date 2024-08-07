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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup.StatsSource.AWR;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup.StatsSource.STATSPACK;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.QueryGroup.TenantSetup.MULTI_TENANT;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.task.StatsJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoValue
@ParametersAreNonnullByDefault
abstract class OracleSqlList {

  abstract ImmutableList<String> names();

  abstract QueryGroup group();

  private static final ImmutableList<String> AWR_NAMES =
      ImmutableList.of("hist-cmd-types-awr", "source-conn-latest", "sql-stats-awr");
  private static final ImmutableList<String> STATSPACK_NAMES =
      ImmutableList.of("hist-cmd-types-statspack", "sql-stats-statspack");

  private static OracleSqlList AWR_CDB =
      create(QueryGroup.create(/* required= */ false, AWR, MULTI_TENANT), AWR_NAMES);
  private static OracleSqlList AWR_DBA =
      create(QueryGroup.create(/* required= */ true, AWR, MULTI_TENANT), AWR_NAMES);
  private static OracleSqlList STATSPACK_CDB =
      create(QueryGroup.create(/* required= */ false, STATSPACK, MULTI_TENANT), STATSPACK_NAMES);

  static OracleSqlList awrCdb() {
    return AWR_CDB;
  }

  static OracleSqlList awrDba() {
    return AWR_DBA;
  }

  static OracleSqlList statspack() {
    return STATSPACK_CDB;
  }

  private static OracleSqlList create(QueryGroup group, ImmutableList<String> names) {
    return new AutoValue_OracleSqlList(names, group);
  }

  ImmutableList<Task<?>> toTasks(Duration queriedDuration) {
    Function<String, Task<?>> mapper =
        x -> {
          OracleStatsQuery query = OracleStatsQuery.create(x, group(), queriedDuration);
          return StatsJdbcTask.fromQuery(query);
        };
    return names().stream().map(mapper).collect(toImmutableList());
  }

  ImmutableList<Task<?>> toTasksIfAllSkipped(
      Duration queriedDuration, List<Task<?>> skippableTasks) {
    Function<String, Task<?>> mapper =
        x -> {
          OracleStatsQuery query = OracleStatsQuery.create(x, group(), queriedDuration);
          return StatsJdbcTask.onlyIfAllSkipped(query, skippableTasks);
        };
    return names().stream().map(mapper).collect(toImmutableList());
  }
}
