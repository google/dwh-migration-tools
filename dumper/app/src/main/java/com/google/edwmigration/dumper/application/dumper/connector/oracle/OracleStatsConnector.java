/*
 * Copyright 2022-2025 Google LLC
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

import static java.time.Duration.ofDays;

import com.google.auto.service.AutoService;
import com.google.common.collect.Range;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoService(Connector.class)
@Description("Dumps aggregated statistics from Oracle")
@ParametersAreNonnullByDefault
public class OracleStatsConnector extends AbstractOracleConnector {

  static final Duration DEFAULT_DURATION = ofDays(30);
  static final Duration MAX_DURATION = ofDays(10000);

  public OracleStatsConnector() {
    super(OracleConnectorScope.STATS);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
    StatsTaskListGenerator taskListGenerator = new StatsTaskListGenerator();
    Duration queriedDuration = getQueriedDuration(arguments);
    out.addAll(taskListGenerator.createTasks(arguments, queriedDuration));
  }

  static Duration getQueriedDuration(ConnectorArguments arguments) {
    Duration queriedDuration = extractFromArgs(arguments);
    if (Range.closed(ofDays(1), MAX_DURATION).contains(queriedDuration)) {
      return queriedDuration;
    } else {
      throw new MetadataDumperUsageException(
          String.format(
              "The number of days must be positive and not greater than %s. Was: %s",
              MAX_DURATION.toDays(), queriedDuration.toDays()));
    }
  }

  private static Duration extractFromArgs(ConnectorArguments arguments) {
    if (arguments.getQueryLogDays() == null) {
      return DEFAULT_DURATION;
    } else {
      int queriedDays = arguments.getQueryLogDays();
      return ofDays(queriedDays);
    }
  }

  @Override
  @Nonnull
  public AssessmentSupport assessmentSupport() {
    return AssessmentSupport.REQUIRED;
  }
}
