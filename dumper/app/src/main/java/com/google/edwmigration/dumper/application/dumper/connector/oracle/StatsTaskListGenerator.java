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

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.AWR;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.STATSPACK;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class StatsTaskListGenerator {

  private final OracleConnectorScope scope = OracleConnectorScope.STATS;

  private static final ImmutableList<String> AWR_NAMES =
      ImmutableList.of("hist-cmd-types-awr", "source-conn-latest", "sql-stats-awr");

  private static final ImmutableList<String> NATIVE_NAMES =
      ImmutableList.of(
          "data-types",
          "db-features",
          "db-instances",
          "db-objects",
          // The version of db-objects that gets SYNONYM objects, for which owner is PUBLIC.
          // A JOIN is performed to exclude objects which appear in the cdb_synonyms table.
          "db-objects-synonym-public",
          "dtl-source-code",
          "external-tables",
          "m-view-types",
          "pdbs-info",
          "app-schemas-pdbs",
          "app-schemas-summary",
          "table-types-dtl",
          "used-space-details");

  private static final ImmutableList<String> STATSPACK_NAMES =
      ImmutableList.of("hist-cmd-types-statspack", "sql-stats-statspack");

  @Nonnull
  ImmutableList<Task<?>> createTasks(ConnectorArguments arguments) throws IOException {
    ImmutableList.Builder<Task<?>> builder = ImmutableList.<Task<?>>builder();
    builder.add(new DumpMetadataTask(arguments, scope.formatName()));
    builder.add(new FormatTask(scope.formatName()));
    for (String name : awrNames()) {
      OracleStatsQuery item = OracleStatsQuery.create(name, AWR);
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    for (String name : nativeNames()) {
      OracleStatsQuery item = OracleStatsQuery.create(name, NATIVE);
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    for (String name : statspackNames()) {
      OracleStatsQuery query = OracleStatsQuery.create(name, STATSPACK);
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

  ImmutableList<String> nativeNames() {
    return NATIVE_NAMES;
  }

  ImmutableList<String> statspackNames() {
    return STATSPACK_NAMES;
  }
}
