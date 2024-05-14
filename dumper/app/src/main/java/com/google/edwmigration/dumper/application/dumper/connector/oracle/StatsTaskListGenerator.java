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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
class StatsTaskListGenerator {

  @Nonnull
  ImmutableList<Task<?>> createTasks(ConnectorArguments arguments) throws IOException {
    ImmutableList.Builder<Task<?>> builder = ImmutableList.<Task<?>>builder();

    ImmutableList<StatsQuery> queries =
        ImmutableList.of(
            // TODO: add entries for other SQLs to this list
            StatsQuery.create("hist-cmd-types", Tool.STATSPACK));
    for (StatsQuery item : queries) {
      builder.add(item.toTask());
    }
    return builder.build();
  }

  enum Tool {
    AWR("awr"),
    STATSPACK("statspack");

    final String value;

    Tool(String value) {
      this.value = value;
    }
  }

  @AutoValue
  abstract static class StatsQuery {

    abstract String name();

    abstract String tool();

    static StatsQuery create(String name, Tool tool) {
      String toolName = tool.value;
      return new AutoValue_StatsTaskListGenerator_StatsQuery(name, toolName);
    }

    @Nonnull
    Task<?> toTask() throws IOException {
      String path = String.format("oracle-stats/%s/%s", tool(), name());
      URL queryUrl = Resources.getResource(path + ".sql");
      String query = Resources.toString(queryUrl, Charset.forName("UTF-8"));
      return new JdbcSelectTask(path + ".csv", query);
    }
  }
}
