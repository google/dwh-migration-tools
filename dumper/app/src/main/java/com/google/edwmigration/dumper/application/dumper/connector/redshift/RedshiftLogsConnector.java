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
package com.google.edwmigration.dumper.application.dumper.connector.redshift;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectIntervalTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.ParallelTaskGroup;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftLogsDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RedshiftMetadataDumpFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates these csv. 1. userid => username mapping ( duplicated from schema ) 2. STL_DDLTEXT for
 * DDLs 3. STL_QUERY and STL_QUERYTEXT for non-DDLs
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Amazon Redshift.")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = RedshiftUrlUtil.OPT_PORT_DEFAULT)
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class RedshiftLogsConnector extends AbstractRedshiftConnector
    implements LogsConnector, RedshiftLogsDumpFormat {

  private static final Logger logger = LoggerFactory.getLogger(RedshiftLogsConnector.class);

  public RedshiftLogsConnector() {
    super("redshift-logs");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments)
      throws MetadataDumperUsageException {

    ParallelTaskGroup.Builder parallelTask = new ParallelTaskGroup.Builder(this.getName());

    //  is also be there in the metadata , no harm is making zip self-sufficient
    parallelTask.addTask(
        new JdbcSelectTask(
            RedshiftMetadataDumpFormat.PgUser.ZIP_ENTRY_NAME, "select * from pg_user"));

    // DDL TEXT is simple ...
    // min() as there is no ANY() or SOME()
    String queryTemplateDDL =
        " SELECT"
            + " any_value(L.userid) AS UserId, any_value(L.starttime) AS StartTime,"
            + " any_value(L.endtime) AS EndTime, any_value(trim(label)) AS Label,"
            + " L.xid AS xid, L.pid AS pid,"
            + " LISTAGG(CASE WHEN LEN(RTRIM(L.text)) = 0 THEN text ELSE RTRIM(L.text) END)"
            + "   WITHIN GROUP (ORDER BY L.sequence) AS SqlText"
            + " FROM STL_DDLTEXT L"
            + " WHERE ##"
            + " GROUP BY L.xid, L.pid ";
    makeTasks(
        arguments,
        RedshiftLogsDumpFormat.DdlHistory.ZIP_ENTRY_PREFIX,
        queryTemplateDDL,
        "L.starttime",
        parallelTask);

    // Query Text has bit of playing around
    // 1. STL_QUERY has starttime, queryid, but text is 4000 char wich is useless
    // 2. STL_QUERY_TEXT has xid+squence+text which reconstructs query, but no starttime.
    // I think STL_QUERY is 1 row per query ; SQL_QUERY_TEXT is multi rows per query, using sequence
    // and xid
    String queryTemplateQuery =
        "WITH qt AS ("
            + " SELECT DISTINCT userid, xid, pid, query,"
            + " LISTAGG(CASE WHEN LEN(RTRIM(text)) = 0 THEN text ELSE RTRIM(text) END)"
            + " WITHIN GROUP (ORDER BY sequence) OVER (PARTITION by userid, xid, pid, query)"
            + " AS SqlText"
            + " FROM STL_QUERYTEXT"
            + ")"
            + " SELECT Q.query AS \"QueryID\", Q.xid AS xid , Q.pid AS pid, Q.userid AS UserId,"
            + " Q.starttime AS StartTime, Q.endtime AS EndTime, trim(Q.label) AS Label,"
            + " QT.SqlText AS SqlText"
            + " FROM STL_QUERY Q JOIN QT"
            + " USING (userid, xid, pid, query) WHERE ##";

    makeTasks(
        arguments,
        RedshiftLogsDumpFormat.QueryHistory.ZIP_ENTRY_PREFIX,
        queryTemplateQuery,
        "Q.starttime",
        parallelTask);

    out.add(parallelTask.build());
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
  }

  // ##  in the template to be replaced by the complete WHERE clause.
  private void makeTasks(
      ConnectorArguments arguments,
      String filePrefix,
      String queryTemplate,
      String startField,
      ParallelTaskGroup.Builder out)
      throws MetadataDumperUsageException {

    List<String> whereClauses = new ArrayList<>();
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
      whereClauses.add(
          String.format(
              "%s >= CAST( '%s' as TIMESTAMP)",
              startField, arguments.getQueryLogEarliestTimestamp()));

    ZonedIntervalIterable intervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(arguments);

    logger.info("Exporting query log for " + intervals);
    for (ZonedInterval interval : intervals) {
      String query =
          queryTemplate.replace(
              "##",
              newWhereClause(
                  whereClauses,
                  String.format(
                      "%s >= CAST('%s' AS TIMESTAMP)",
                      startField, SQL_FORMAT.format(interval.getStart())),
                  String.format(
                      "%s < CAST('%s' AS TIMESTAMP)",
                      startField, SQL_FORMAT.format(interval.getEndExclusive()))));
      String file =
          filePrefix
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + RedshiftLogsDumpFormat.ZIP_ENTRY_SUFFIX;
      out.addTask(new JdbcSelectIntervalTask(file, query, interval));
    }
  }
}
