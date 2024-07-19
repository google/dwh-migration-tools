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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class StatsTaskListGenerator {

  private final OracleConnectorScope scope = OracleConnectorScope.STATS;

  private static final ImmutableList<String> AWR_NAMES =
      ImmutableList.of("hist-cmd-types-awr", "source-conn-latest", "sql-stats-awr");

  private static final ImmutableList<String> NATIVE_NAMES_OPTIONAL =
      ImmutableList.of(
          "app-schemas-pdbs",
          "app-schemas-summary",
          "db-features",
          "db-instances",
          "dtl-index-type",
          "dtl-source-code",
          "exttab",
          "m-view-types",
          "pdbs-info");

  private static final ImmutableList<String> NATIVE_NAMES_REQUIRED =
      ImmutableList.of(
          "data-types",
          "db-info",
          "db-objects",
          // The version of db-objects that gets SYNONYM objects, for which owner is PUBLIC.
          // A JOIN is performed to exclude objects which appear in the cdb_synonyms table.
          "db-objects-synonym-public",
          "table-types-dtl",
          "used-space-details");

  private static final ImmutableList<String> STATSPACK_NAMES =
      ImmutableList.of("hist-cmd-types-statspack", "sql-stats-statspack");

  @Nonnull
  ImmutableList<Task<?>> createTasks(ConnectorArguments arguments, Duration queriedDuration) {
    ImmutableList.Builder<Task<?>> builder = ImmutableList.<Task<?>>builder();
    builder.add(new DumpMetadataTask(arguments, scope.formatName()));
    builder.add(new FormatTask(scope.formatName()));
    for (String name : awrNames()) {
      OracleStatsQuery item = OracleStatsQuery.createAwr(name, queriedDuration);
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    for (String name : nativeNames(/* required= */ true)) {
      OracleStatsQuery item =
          OracleStatsQuery.createNative(name, /* isRequired= */ true, queriedDuration);
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    for (String name : nativeNames(/* required= */ false)) {
      OracleStatsQuery item =
          OracleStatsQuery.createNative(name, /* isRequired= */ false, queriedDuration);
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    for (String name : statspackNames()) {
      OracleStatsQuery query = OracleStatsQuery.createStatspack(name, queriedDuration);
      builder.add(StatsJdbcTask.fromQuery(query));
    }
    return builder.build();
  }

  /** The source of performance statistics. */
  enum StatsSource {
    AWR("awr"),
    NATIVE("native"),
    STATSPACK("statspack");

    final String value;

    StatsSource(String value) {
      this.value = value;
    }
  }

  ImmutableList<String> awrNames() {
    return AWR_NAMES;
  }

  ImmutableList<String> nativeNames(boolean required) {
    return required ? NATIVE_NAMES_REQUIRED : NATIVE_NAMES_OPTIONAL;
  }

  ImmutableList<String> statspackNames() {
    return STATSPACK_NAMES;
  }
}
