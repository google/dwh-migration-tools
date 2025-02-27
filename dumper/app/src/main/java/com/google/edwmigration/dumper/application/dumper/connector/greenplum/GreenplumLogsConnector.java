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
package com.google.edwmigration.dumper.application.dumper.connector.greenplum;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.IntervalExpander;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectIntervalTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.ParallelTaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.GreenplumLogsDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.GreenplumMetadataDumpFormat;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Greenplum")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + GreenplumLogsConnector.OPT_PORT_DEFAULT)
@RespectsArgumentAssessment
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class GreenplumLogsConnector extends AbstractGreenplumConnector
    implements LogsConnector, GreenplumLogsDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(GreenplumLogsConnector.class);

  public GreenplumLogsConnector() {
    super("greenplum-logs");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments)
      throws MetadataDumperUsageException {

    ParallelTaskGroup parallelTask = new ParallelTaskGroup(this.getName());
    out.add(parallelTask);

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));

    // pg_user table is also dumped in the greenplum(metadata) connector, but there is no harm
    // making this zip self-sufficient.
    parallelTask.addTask(
        new JdbcSelectTask(
            GreenplumMetadataDumpFormat.PgUser.ZIP_ENTRY_NAME, "select * from pg_user"));

    Duration rotationDuration = arguments.getQueryLogRotationFrequency();
    ZonedIntervalIterable intervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, rotationDuration, IntervalExpander.createBasedOnDuration(rotationDuration));

    String queryQueueHistoryTemplateQuery =
        "SELECT ctime, tmid, ssid, ccnt, username, db, cost, tsubmit, tstart,  tfinish, status,"
            + " rows_out, cpu_elapsed, cpu_currpct, skew_cpu, skew_rows, query_hash,  query_text,"
            + " query_plan, application_name, rsqname, rqppriority FROM queries_history WHERE ##";
    makeTasks(
        arguments,
        intervals,
        GreenplumLogsDumpFormat.QueryHistory.ZIP_ENTRY_PREFIX,
        queryQueueHistoryTemplateQuery,
        "tstart",
        parallelTask);
  }

  // ## in the template to be replaced by the complete WHERE clause.
  private void makeTasks(
      ConnectorArguments arguments,
      ZonedIntervalIterable intervals,
      String filePrefix,
      String queryTemplate,
      String startField,
      ParallelTaskGroup out)
      throws MetadataDumperUsageException {

    for (ZonedInterval interval : intervals) {
      String query =
          queryTemplate.replace(
              "##",
              newWhereClause(
                  String.format(
                      "%s >= TIMESTAMP '%s'", startField, SQL_FORMAT.format(interval.getStart())),
                  String.format(
                      "%s < TIMESTAMP '%s'",
                      startField, SQL_FORMAT.format(interval.getEndExclusive()))));
      String file =
          filePrefix
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + GreenplumLogsDumpFormat.ZIP_ENTRY_SUFFIX;
      out.addTask(new JdbcSelectIntervalTask(file, query, interval));
    }
  }
}
