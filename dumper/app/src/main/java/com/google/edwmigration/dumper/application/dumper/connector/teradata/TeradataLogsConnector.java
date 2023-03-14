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
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
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

  public TeradataLogsConnector() {
    super("teradata-logs");
  }

  // to proxy for Terdata14LogsConnector
  protected TeradataLogsConnector(@Nonnull String name) {
    super(name);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    TeradataSqlQueryFactoryBuilder builder =
        TeradataSqlQueryFactoryBuilder.startBuildingFrom(arguments);
    Class<? extends Enum<?>> headerClass =
        arguments.isAssessment() ? HeaderForAssessment.class : Header.class;

    // Beware of Teradata SQLSTATE HY000. See issue #4126.
    // Most likely caused by some operation (equality?) being performed on a datum which is too long
    // for a varchar.
    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
    LOG.info("Exporting query log for " + intervals);
    SharedState state = new SharedState();
    for (ZonedInterval interval : intervals) {
      String file =
          ZIP_ENTRY_PREFIX
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      SqlQueryFactory factory = builder.within(interval).build();
      out.add(new TeradataLogsJdbcTask(file, state, factory).withHeaderClass(headerClass));
    }
  }

  /** This is shared between all instances of TeradataLogsJdbcTask. */
  /* pp */ static class SharedState {

    /**
     * Whether a particular expression is valid against the particular target Teradata version. This
     * is a concurrent Map of immutable objects, so is threadsafe overall.
     */
    /* pp */ final ConcurrentMap<String, Boolean> expressionValidity = new ConcurrentHashMap<>();
  }
}
