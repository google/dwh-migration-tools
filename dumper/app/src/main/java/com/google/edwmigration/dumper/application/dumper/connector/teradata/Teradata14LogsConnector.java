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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.SqlQueryFactory;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version <=14.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class Teradata14LogsConnector extends TeradataLogsConnector {

  private static final Logger LOG = LoggerFactory.getLogger(Teradata14LogsConnector.class);

  public Teradata14LogsConnector() {
    super("teradata14-logs");
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    TeradataSqlQueryFactoryBuilder lsqlBuilder =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments, true);
    TeradataSqlQueryFactoryBuilder logQueryBuilder =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments);

    final int daysToExport = arguments.getQueryLogDays(7);
    if (daysToExport <= 0)
      throw new MetadataDumperUsageException(
          "At least one day of query logs should be exported; you specified: " + daysToExport);

    // Beware of Teradata SQLSTATE HY000. See issue #4126.
    // Most likely caused by some operation (equality?) being performed on a datum which is too long
    // for a varchar.
    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
    LOG.info("Exporting query log for " + intervals);
    SharedState state = new SharedState();
    for (ZonedInterval interval : intervals) {
      String LSqlfile =
          ZIP_ENTRY_PREFIX_LSQL
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      SqlQueryFactory lsqlQueryFactory = lsqlBuilder.within(interval).build();
      out.add(
          new TeradataLogsJdbcTask(LSqlfile, state, lsqlQueryFactory)
              .withHeaderClass(TeradataLogsDumpFormat.HeaderLSql.class));

      String LOGfile =
          ZIP_ENTRY_PREFIX_LOG
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      SqlQueryFactory logQueryFactory = logQueryBuilder.within(interval).build();
      out.add(
          new TeradataLogsJdbcTask(LOGfile, state, logQueryFactory)
              .withHeaderClass(TeradataLogsDumpFormat.HeaderLog.class));
    }
  }
}
