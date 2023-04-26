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
 * Generates these csv. 1. userid => username mapping ( duplicated from schema ) 2. SVL_DDLTEXT for
 * DDLs 3 SVL_QUERY_TEXT for non-DDLS 4. SQL_QUERY_METRICS for metrics
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Amazon Redshift.")
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + RedshiftMetadataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class RedshiftLogsConnector extends AbstractRedshiftConnector
    implements LogsConnector, RedshiftLogsDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(RedshiftLogsConnector.class);

  public RedshiftLogsConnector() {
    super("redshift-logs");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments)
      throws MetadataDumperUsageException {

    ParallelTaskGroup parallelTask = new ParallelTaskGroup(this.getName());
    out.add(parallelTask);

    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

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

    if (false) {
      // Metric Dance.... S3 logs has xid ..  .. so Above wil have to be used to map xid's to query
      //  1. SVL_QUERY_METRICS doesn't have starttime
      //  2. so join with QTL_QUERY....
      String queryTemplateMetrics =
          " SELECT Q.query as \"QueryID\", Q.xid, Q.starttime, QM.* "
              + " FROM STL_QUERY Q JOIN SVL_QUERY_METRICS QM USING (query) WHERE ##";

      makeTasks(
          arguments,
          RedshiftLogsDumpFormat.ZIP_ENTRY_PREFIX_METRICS,
          queryTemplateMetrics,
          "Q.starttime",
          parallelTask);

      // We want to know how many bytes were scanned per query, so we must consult STL_SCAN;
      // SVL_QUERY_METRICS already gives us "scan_row_count", but doesn't tell us anything about
      // bytes scanned.
      String queryTemplateScan =
          " SELECT Q.query as \"QueryID\", min(Q.starttime) as starttime, sum(QS.rows) as"
              + " stl_scan_sum_rows, sum(QS.bytes) as stl_scan_sum_bytes  FROM STL_QUERY Q USING"
              + " STL_SCAN QS USING (query) WHERE ## GROUP BY Q.query";

      makeTasks(
          arguments,
          RedshiftLogsDumpFormat.ZIP_ENTRY_PREFIX_SCANS,
          queryTemplateScan,
          "Q.starttime",
          parallelTask);
    }
  }

  // ##  in the template to be replaced by the complete WHERE clause.
  private void makeTasks(
      ConnectorArguments arguments,
      String filePrefix,
      String queryTemplate,
      String startField,
      ParallelTaskGroup out)
      throws MetadataDumperUsageException {

    List<String> whereClauses = new ArrayList<>();
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
      whereClauses.add(
          String.format(
              "%s >= CAST( '%s' as TIMESTAMP)",
              startField, arguments.getQueryLogEarliestTimestamp()));

    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);

    LOG.info("Exporting query log for " + intervals);
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
