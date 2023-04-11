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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
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
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matt
 *     <p>TODO :Make a base class, and derive TeradataLogs and TeradataLogs14 from it
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version >=15.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
@RespectsArgumentAssessment
public class TeradataLogsConnector extends AbstractTeradataConnector
    implements LogsConnector, TeradataLogsDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);
  private static final String ASSESSMENT_DEF_LOG_TABLE = "dbc.QryLogV";

  private static final String DEF_UTILITY_TABLE = "dbc.DBQLUtilityTbl";

  public TeradataLogsConnector() {
    super("teradata-logs");
  }

  private ImmutableList<TeradataJdbcSelectTask> createTimeSeriesTasks(ZonedInterval interval) {
    return ImmutableList.of("ResUsageScpu", "ResUsageSpma").stream()
        .map(
            tableName ->
                new TeradataJdbcSelectTask(
                    createFilename("dbc." + tableName + "_", interval),
                    TaskCategory.OPTIONAL,
                    String.format(
                        "SELECT %%s FROM DBC.%s WHERE TheTimestamp >= %s AND TheTimestamp < %s",
                        tableName,
                        interval.getStart().toEpochSecond(),
                        interval.getEndExclusive().toEpochSecond())))
        .collect(toImmutableList());
  }

  private String createFilename(String zipEntryPrefix, ZonedInterval interval) {
    return zipEntryPrefix
        + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
        + ".csv";
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    boolean isAssessment = arguments.isAssessment();
    String logTable = isAssessment ? ASSESSMENT_DEF_LOG_TABLE : DEF_LOG_TABLE;
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
    List<String> conditions = new ArrayList<>();
    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp())) {
      conditions.add("L.StartTime >= " + arguments.getQueryLogEarliestTimestamp());
    }

    // Beware of Teradata SQLSTATE HY000. See issue #4126.
    // Most likely caused by some operation (equality?) being performed on a datum which is too long
    // for a varchar.
    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
    LOG.info("Exporting query log for " + intervals);
    SharedState queryLogsState = new SharedState();
    SharedState utilityLogsState = new SharedState();
    for (ZonedInterval interval : intervals) {
      String file = createFilename(ZIP_ENTRY_PREFIX, interval);
      if (isAssessment) {
        List<String> orderBy = Arrays.asList("ST.QueryID", "ST.SQLRowNo");
        out.add(
            new TeradataAssessmentLogsJdbcTask(
                    file, queryLogsState, logTable, queryTable, conditions, interval, orderBy)
                .withHeaderClass(HeaderForAssessment.class));
        out.addAll(createTimeSeriesTasks(interval));
        out.add(
            new TeradataUtilityLogsJdbcTask(
                createFilename("utility_logs_", interval),
                utilityLogsState,
                DEF_UTILITY_TABLE,
                interval));
      } else {
        conditions.add("L.UserName <> 'DBC'");
        out.add(
            new TeradataLogsJdbcTask(
                    file, queryLogsState, logTable, queryTable, conditions, interval)
                .withHeaderClass(Header.class));
      }
    }
  }
}
