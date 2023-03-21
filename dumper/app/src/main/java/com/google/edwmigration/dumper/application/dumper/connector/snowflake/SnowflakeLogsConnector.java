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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeLogsDumpFormat;
import com.google.errorprone.annotations.ForOverride;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Snowflake.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class SnowflakeLogsConnector extends AbstractSnowflakeConnector
    implements LogsConnector, SnowflakeLogsDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeLogsConnector.class);

  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  protected SnowflakeLogsConnector(@Nonnull String name) {
    super(name);
  }

  public SnowflakeLogsConnector() {
    this("snowflake-logs");
  }

  public enum SnowflakeLogConnectorProperties implements ConnectorProperty {
    OVERRIDE_QUERY("snowflake.logs.query", "Custom query for log dump."),
    OVERRIDE_WHERE(
        "snowflake.logs.where", "Custom where condition to append to query for log dump.");

    private final String name;
    private final String description;

    SnowflakeLogConnectorProperties(String name, String description) {
      this.name = name;
      this.description = description;
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description;
    }
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return SnowflakeLogConnectorProperties.class;
  }

  @ForOverride
  protected String newQueryFormat(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    // Docref: https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html
    // Per the docref, Snowflake only retains/returns seven trailing days of logs.
    String overrideQuery = getOvverrideQuery(arguments);
    if (overrideQuery != null) return overrideQuery;

    String overrideWhere = getOverrideWhere(arguments);

    @SuppressWarnings("OrphanedFormatString")
    StringBuilder queryBuilder =
        new StringBuilder(
            "SELECT database_name, \n"
                + "schema_name, \n"
                + "user_name, \n"
                + "warehouse_name, \n"
                + "execution_status, \n"
                + "error_code, \n"
                + "start_time, \n"
                + "end_time, \n"
                + "total_elapsed_time, \n"
                + "bytes_scanned, \n"
                + "rows_produced, \n"
                + "credits_used_cloud_services, \n"
                + "query_text \n"
                + "FROM table(INFORMATION_SCHEMA.QUERY_HISTORY(\n"
                + "result_limit=>10000\n"
                // maximum range of 7 trailing days.
                + ",end_time_range_start=>to_timestamp_ltz('%s')\n"
                + ",end_time_range_end=>to_timestamp_ltz('%s')\n"
                + "))\n");
    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    // TODO: Use ZonedIntervalIterable.forConnectorArguments()
    boolean appendStartTime = !StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp());
    if (appendStartTime)
      queryBuilder
          .append("WHERE start_time >= ")
          .append(arguments.getQueryLogEarliestTimestamp())
          .append("\n");
    if (overrideWhere != null)
      queryBuilder.append(appendStartTime ? " AND " : "WHERE").append(overrideWhere);
    return queryBuilder.toString().replace('\n', ' ');
  }

  @CheckForNull
  protected String getOvverrideQuery(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    String overrideQuery = arguments.getDefinition(SnowflakeLogConnectorProperties.OVERRIDE_QUERY);
    if (overrideQuery != null) {
      if (StringUtils.countMatches(overrideQuery, "%s") != 2)
        throw new MetadataDumperUsageException(
            "Custom query for log dump needs two \"%s\" expansions, they will be expanded to end_time lower and upper boundaries.");
      return overrideQuery;
    }
    return null;
  }

  @CheckForNull
  protected String getOverrideWhere(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    return arguments.getDefinition(SnowflakeLogConnectorProperties.OVERRIDE_WHERE);
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    // (24 * 7) -> 7 trailing days == 168 hours
    // Actually, on Snowflake, 7 days ago starts at midnight in an unadvertised time zone. What the
    // <deleted>.
    // Snowflake will refuse (CURRENT_TIMESTAMP - 168 hours) because it is beyond the
    // 7-day window allowed by the server-side function.
    ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
    LOG.info("Exporting query log for " + intervals);
    for (ZonedInterval interval : intervals) {
      String query =
          String.format(
              newQueryFormat(arguments),
              SQL_FORMAT.format(interval.getStart()),
              SQL_FORMAT.format(interval.getEndInclusive()));
      String file =
          ZIP_ENTRY_PREFIX
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      out.add(new JdbcSelectTask(file, query).withHeaderClass(Header.class));
    }
  }
}
