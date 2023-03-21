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
package com.google.edwmigration.dumper.application.dumper.connector.generic;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.sql.Driver;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * there is no generics-logs and generic-meta .. it's the same if start & end dates are given, it
 * generates multiple queries and is like 'logs' mode.
 */
@AutoService({Connector.class, LogsConnector.class})
public class GenericConnector extends AbstractJdbcConnector implements LogsConnector {

  private static final Logger LOG = LoggerFactory.getLogger(GenericConnector.class);

  public static final String NAME = "generic";
  public static final String PERIOD_START = "{period-start}";
  public static final String PERIOD_END = "{period-end}";
  public static final String PERIOD_NAME = "{period}";

  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  // private boolean argumentsProcessed = false;
  private String uri;
  private List<String> driverPaths;
  private String driverClass;
  private String entryName;
  private String query;
  private boolean multiMode; // true if a start-time and end-time is provided.

  public GenericConnector() {
    super(NAME);
  }

  private void checkArguments(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    // interefers with testing, no harm in calling two times anyway
    // if (argumentsProcessed)
    //    return;
    // argumentsProcessed = true;

    List<String> errors = new ArrayList<>();

    this.uri = arguments.getUri();
    if (this.uri == null) errors.add("--url is required for generic-logs connector");
    this.driverPaths = arguments.getDriverPaths();
    this.driverClass = arguments.getDriverClass();
    if (this.driverClass == null) errors.add("--driver is required for generic-logs connector");

    this.query = arguments.getGenericQuery();
    if (this.query == null) errors.add("--generic-query is required for generic-logs connector");

    // presence of --query-log-start  or --query-log-end indicate user wants a multi-entry zip
    this.entryName = arguments.getGenericEntry();

    this.multiMode = arguments.getQueryLogEnd() != null || arguments.getQueryLogStart() != null;

    if (this.multiMode) {
      if (arguments.getQueryLogStart()
          == null) // no check for --query-log-end because it will be defaulted by
        // ZonedIntervalIterable if absent
        errors.add("Missing option --query-log-start must be specified.");
      if (this.entryName == null) this.entryName = "generic-logs-{period}.csv";
      if (this.query != null) {
        if (!this.query.contains(PERIOD_START))
          errors.add("Query string must contain where clause with date_column >= " + PERIOD_START);
        if (!this.query.contains(PERIOD_END))
          errors.add("Query string must contain where clause with date_column < " + PERIOD_END);
      }
      if (this.entryName != null) {
        if (!this.entryName.contains(PERIOD_NAME))
          errors.add("Entry name must have " + PERIOD_NAME + " in the name");
      }
    } else {
      if (this.entryName == null) this.entryName = "generic-query.csv";
    }

    if (!errors.isEmpty())
      throw new MetadataDumperUsageException(
          "Missing arguments for connector generic-args :", errors);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) throws Exception {
    checkArguments(arguments);

    out.add(new DumpMetadataTask(arguments, "generic.dump.zip"));
    out.add(new FormatTask("generic.dump.zip"));

    if (multiMode) {
      ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
      for (ZonedInterval interval : intervals) {
        String finalQuery =
            this.query
                .replace("{period-start}", "'" + SQL_FORMAT.format(interval.getStart()) + "'")
                .replace("{period-end}", "'" + SQL_FORMAT.format(interval.getEndInclusive()) + "'");
        String finalFile =
            this.entryName.replace(
                "{period}", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC()));
        out.add(new JdbcSelectTask(finalFile, finalQuery));
      }

    } else {
      LOG.debug("Task: {} , {}", this.entryName, this.query);
      out.add(new JdbcSelectTask(this.entryName, this.query));
    }
  }

  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    checkArguments(arguments);

    Driver driver = newDriver(this.driverPaths, this.driverClass);
    DataSource dataSource = newSimpleDataSource(driver, this.uri, arguments);
    return JdbcHandle.newPooledJdbcHandle(dataSource, 2);
  }
}
