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
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoService(Connector.class)
@Description("Dumps aggregated statistics from Oracle")
@ParametersAreNonnullByDefault
public class OracleStatsConnector extends AbstractOracleConnector {

  public OracleStatsConnector() {
    super(OracleConnectorScope.STATS);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
    StatsTaskListGenerator taskListGenerator = new StatsTaskListGenerator();
    out.add(new DumpMetadataTask(arguments, getFormatName()));
    out.addAll(taskListGenerator.createTasks(arguments));
  }
}
