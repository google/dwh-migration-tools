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
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class StatsTaskListGenerator {

  @Nonnull
  ImmutableList<Task<?>> createTasks(ConnectorArguments arguments) throws IOException {
    ImmutableList<OracleStatsQuery> queries =
        ImmutableList.of(
            OracleStatsQuery.create("hist-cmd-types", StatsSource.STATSPACK),
            OracleStatsQuery.create("app-schemas-pdbs", StatsSource.METADATA),
            OracleStatsQuery.create("app-schemas-summary", StatsSource.METADATA)
            // TODO: add entries for other SQLs to this list
            );

    ImmutableList.Builder<Task<?>> builder = ImmutableList.<Task<?>>builder();

    for (OracleStatsQuery item : queries) {
      builder.add(StatsJdbcTask.fromQuery(item));
    }
    return builder.build();
  }

  /** The source of performance statistics. */
  enum StatsSource {
    AWR("awr"),
    METADATA("metadata"),
    STATSPACK("statspack");

    final String value;

    StatsSource(String value) {
      this.value = value;
    }
  }
}
