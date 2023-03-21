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
package com.google.edwmigration.dumper.application.dumper.connector.vertica;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.VerticaLogsDumpFormat;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Vertica.")
public class VerticaLogsConnector extends AbstractVerticaConnector
    implements LogsConnector, VerticaLogsDumpFormat {

  public VerticaLogsConnector() {
    super("vertica-logs");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    // Docref:
    // https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/AnalyzingData/Optimizations/OptimizingDCTableQueries.htm
    //
    // https://forum.vertica.com/discussion/207345/how-to-find-all-statements-executed-by-any-user
    // Importantly, dc_requests_issued is the only relation containing query text,
    // but such queries may not have been successful, hence the join on dc_requests_completed.

    String startTimestamp = arguments.getQueryLogEarliestTimestamp();

    // Shevek checked on PostgreSQL, and it does not appear that `time` needs quoting here.
    StringBuilder dc_requests_issued = new StringBuilder("SELECT * FROM dc_requests_issued");
    if (!StringUtils.isBlank(startTimestamp))
      dc_requests_issued.append(" WHERE time >= ").append(startTimestamp);
    out.add(
        new JdbcSelectTask(
            ZIP_ENTRY_PREFIX + "dc_requests_issued.csv", dc_requests_issued.toString()));

    StringBuilder dc_requests_completed = new StringBuilder("SELECT * FROM dc_requests_completed");
    if (!StringUtils.isBlank(startTimestamp))
      dc_requests_completed.append(" WHERE time >= ").append(startTimestamp);
    out.add(
        new JdbcSelectTask(
            ZIP_ENTRY_PREFIX + "dc_requests_completed.csv", dc_requests_completed.toString()));
  }
}
