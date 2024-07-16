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

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoService(Connector.class)
@Description("Dumps aggregated statistics from Oracle")
@ParametersAreNonnullByDefault
public class OracleStatsConnector extends AbstractOracleConnector {

  static int MAX_DAYS = 10000;

  public OracleStatsConnector() {
    super(OracleConnectorScope.STATS);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
    StatsTaskListGenerator taskListGenerator = new StatsTaskListGenerator();
    int queriedDays = getQueryLogDays(arguments);
    out.addAll(taskListGenerator.createTasks(arguments, queriedDays));
  }

  @Nonnull
  @Override
  public String summary(String fileName) {
    return String.format("Oracle statistics saved to %s", fileName);
  }

  static int getQueryLogDays(ConnectorArguments arguments) {
    int queriedDays;
    if (arguments.getQueryLogDays() == null) {
      queriedDays = 30;
    } else {
      queriedDays = arguments.getQueryLogDays();
    }
    if (queriedDays > 0 && queriedDays <= MAX_DAYS) {
      return queriedDays;
    } else {
      throw invalidDuration(queriedDays);
    }
  }

  private static MetadataDumperUsageException invalidDuration(int days) {
    String message =
        String.format(
            "The number of days must be positive and not greater than %s. Was: %s", MAX_DAYS, days);
    return new MetadataDumperUsageException(message);
  }
}
