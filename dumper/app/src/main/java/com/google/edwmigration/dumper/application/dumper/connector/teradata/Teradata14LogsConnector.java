/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version <=14.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class Teradata14LogsConnector extends TeradataLogsConnector {

  private static final Logger LOG = LoggerFactory.getLogger(Teradata14LogsConnector.class);

  @VisibleForTesting
  static final List<String> EXPRESSIONS_LSQL_TBL = enumNames("ST.",
      TeradataLogsDumpFormat.HeaderLSql.class);

  @VisibleForTesting
  static final List<String> EXPRESSIONS_LOG_TBL = enumNames("L.",
      TeradataLogsDumpFormat.HeaderLog.class);

  private static List<String> enumNames(String prefix, Class<? extends Enum<?>> en) {
    Enum<?> v[] = en.getEnumConstants();
    List<String> ret = new ArrayList<>(v.length);
    for (Enum<?> h : v) {
      ret.add(prefix + h.name());
    }
    return ret;
  }

  public Teradata14LogsConnector() {
    super("teradata14-logs");
  }

  private static class LSqlQueryFactory extends TeradataLogsConnector.TeradataLogsJdbcTask {

    public LSqlQueryFactory(String targetPath, SharedState state, String logTable,
        String queryTable, List<String> conditions, ZonedInterval interval) {
      super(targetPath, state, logTable, queryTable, conditions, interval);
    }

    @Override
    @Nonnull
    String getSql(@Nonnull Predicate<? super String> predicate) {
      StringBuilder buf = new StringBuilder("SELECT ");

      String separator = "";
      for (String expression : EXPRESSIONS_LSQL_TBL) {
        buf.append(separator);
        if (predicate.test(expression)) {
          buf.append(expression);
        } else {
          buf.append("NULL");
        }
        separator = ", ";
      }

      buf.append(" FROM ").append(queryTable).append(" ST ");

      buf.append(String.format("WHERE ST.CollectTimeStamp >= CAST('%s' AS TIMESTAMP)\n"
              + "AND ST.CollectTimeStamp < CAST('%s' AS TIMESTAMP)\n",
          SQL_FORMAT.format(interval.getStart()), SQL_FORMAT.format(interval.getEndExclusive())));

      for (String condition : conditions) {
        buf.append(" AND ").append(condition);
      }

      return buf.toString().replace('\n', ' ');
    }

  }

  private static class LogQueryFactory extends TeradataLogsConnector.TeradataLogsJdbcTask {

    public LogQueryFactory(String targetPath, SharedState state, String logTable, String queryTable,
        List<String> conditions, ZonedInterval interval) {
      super(targetPath, state, logTable, queryTable, conditions, interval);
    }

    @Override
    @Nonnull
    String getSql(@Nonnull Predicate<? super String> predicate) {
      StringBuilder buf = new StringBuilder("SELECT ");

      String separator = "";
      for (String expression : EXPRESSIONS_LOG_TBL) {
        buf.append(separator);
        if (predicate.test(expression)) {
          buf.append(expression);
        } else {
          buf.append("NULL");
        }
        separator = ", ";
      }

      buf.append(" FROM ").append(logTable).append(" L ");

      buf.append(String.format("WHERE L.StartTime >= CAST('%s' AS TIMESTAMP)\n"
              + "AND L.StartTime < CAST('%s' AS TIMESTAMP)\n",
          SQL_FORMAT.format(interval.getStart()), SQL_FORMAT.format(interval.getEndExclusive())));

      for (String condition : conditions) {
        buf.append(" AND ").append(condition);
      }

      return buf.toString().replace('\n', ' ');
    }

  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    String logTable = DEF_LOG_TABLE;
    String queryTable = DEF_QUERY_TABLE;
    List<String> alternates = arguments.getQueryLogAlternates();
    if (!alternates.isEmpty()) {
      if (alternates.size() != 2) {
        throw new MetadataDumperUsageException(
            "Alternate query log tables must be given as a pair; you specified: " + alternates);
      }
      logTable = alternates.get(0);
      queryTable = alternates.get(1);
    }

    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    List<String> lSqlConditions = new ArrayList<>();
    List<String> logConditions = new ArrayList<>();
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp())) {
      lSqlConditions.add("ST.CollectTimeStamp >= " + arguments.getQueryLogEarliestTimestamp());
      logConditions.add("L.StartTime >= " + arguments.getQueryLogEarliestTimestamp());
    }

    final int daysToExport = arguments.getQueryLogDays(7);
    if (daysToExport <= 0) {
      throw new MetadataDumperUsageException(
          "At least one day of query logs should be exported; you specified: " + daysToExport);
    }

    // Beware of Teradata SQLSTATE HY000. See issue #4126.
    // Most likely caused by some operation (equality?) being performed on a datum which is too long for a varchar.
    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
    LOG.info("Exporting query log for " + intervals);
    SharedState state = new SharedState();
    for (ZonedInterval interval : intervals) {
      String LSqlfile = ZIP_ENTRY_PREFIX_LSQL + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
          interval.getStartUTC()) + ".csv";
      out.add(new LSqlQueryFactory(LSqlfile, state, logTable, queryTable, lSqlConditions,
          interval).withHeaderClass(TeradataLogsDumpFormat.HeaderLSql.class));

      String LOGfile = ZIP_ENTRY_PREFIX_LOG + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
          interval.getStartUTC()) + ".csv";
      out.add(new LogQueryFactory(LOGfile, state, logTable, queryTable, logConditions,
          interval).withHeaderClass(TeradataLogsDumpFormat.HeaderLog.class));

    }
  }
}
