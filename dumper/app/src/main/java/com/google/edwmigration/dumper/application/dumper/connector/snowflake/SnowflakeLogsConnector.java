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
import com.google.common.base.CaseFormat;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.TimeTruncator;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeLogsDumpFormat;
import com.google.errorprone.annotations.ForOverride;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
        "snowflake.logs.where", "Custom where condition to append to query for log dump."),

    WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY(
        "snowflake.warehouse_events_history.query",
        "Custom query for warehouse events history dump"),
    AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY(
        "snowflake.automatic_clustering_history.query",
        "Custom query for automatic clustering history dump"),
    COPY_HISTORY_OVERRIDE_QUERY(
        "snowflake.copy_history.query", "Custom query for copy history dump"),
    DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY(
        "snowflake.database_replication_usage_history.query",
        "Custom query for database replication usage history dump"),
    LOGIN_HISTORY_OVERRIDE_QUERY(
        "snowflake.login_history.query", "Custom query for login history dump"),
    METERING_DAILY_HISTORY_OVERRIDE_QUERY(
        "snowflake.metering_daily_history.query", "Custom query for metering daily history dump"),
    PIPE_USAGE_HISTORY_OVERRIDE_QUERY(
        "snowflake.pipe_usage_history.query", "Custom query for pipe usage history dump"),
    QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY(
        "snowflake.query_acceleration_history.query",
        "Custom query for query acceleration history dump"),
    REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY(
        "snowflake.replication_group_usage_history.query",
        "Custom query for replication group usage history dump"),
    SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY(
        "snowflake.serverless_task_history.query", "Custom query for serverless task history dump"),
    TASK_HISTORY_OVERRIDE_QUERY(
        "snowflake.task_history.query", "Custom query for task history dump"),
    WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY(
        "snowflake.warehouse_load_history.query", "Custom query for warehouse load history dump"),
    WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY(
        "snowflake.warehouse_metering_history.query",
        "Custom query for warehouse metering history dump");

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

  private static class TaskDescription {
    private final String zipPrefix;
    private final String unformattedQuery;
    private final Class<? extends Enum<?>> headerClass;

    private TaskDescription(
        String zipPrefix, String unformattedQuery, Class<? extends Enum<?>> headerClass) {
      this.unformattedQuery = unformattedQuery;
      this.zipPrefix = zipPrefix;
      this.headerClass = headerClass;
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
    return createQueryFromInformationSchema(arguments);
  }

  protected String createQueryFromAccountUsage(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    String overrideQuery = getOverrideQuery(arguments);
    if (overrideQuery != null) return overrideQuery;

    String overrideWhere = getOverrideWhere(arguments);

    @SuppressWarnings("OrphanedFormatString")
    StringBuilder queryBuilder =
        new StringBuilder(
            "SELECT database_name, \n"
                + "schema_name, \n"
                + "user_name, \n"
                + "warehouse_name, \n"
                + "query_id, \n"
                + "session_id, \n"
                + "query_type, \n"
                + "execution_status, \n"
                + "error_code, \n"
                + "start_time, \n"
                + "end_time, \n"
                + "total_elapsed_time, \n"
                + "bytes_scanned, \n"
                + "rows_produced, \n"
                + "credits_used_cloud_services, \n"
                + "query_text \n"
                + "FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY\n"
                + "WHERE end_time >= to_timestamp_ltz('%s')\n"
                + "AND end_time <= to_timestamp_ltz('%s')\n");
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
      queryBuilder
          .append("AND start_time >= ")
          .append(arguments.getQueryLogEarliestTimestamp())
          .append("\n");
    if (overrideWhere != null) queryBuilder.append(" AND ").append(overrideWhere);
    return queryBuilder.toString().replace('\n', ' ');
  }

  protected String createQueryFromInformationSchema(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    // Docref: https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html
    // Per the docref, Snowflake only retains/returns seven trailing days of logs in
    // INFORMATION_SCHEMA.
    String overrideQuery = getOverrideQuery(arguments);
    if (overrideQuery != null) return overrideQuery;

    String overrideWhere = getOverrideWhere(arguments);

    @SuppressWarnings("OrphanedFormatString")
    StringBuilder queryBuilder =
        new StringBuilder(
            "SELECT database_name, \n"
                + "schema_name, \n"
                + "user_name, \n"
                + "warehouse_name, \n"
                + "query_id, \n"
                + "session_id, \n"
                + "query_type, \n"
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
    // TODO: Use ZonedIntervalIterableGenerator.forConnectorArguments()
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

  protected String createExtendedQueryFromAccountUsage(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    String overrideQuery = getOverrideQuery(arguments);
    if (overrideQuery != null) return overrideQuery;

    String overrideWhere = getOverrideWhere(arguments);

    @SuppressWarnings("OrphanedFormatString")
    StringBuilder queryBuilder =
        new StringBuilder(
            "SELECT query_id, \n"
                + "query_text, \n"
                + "database_name, \n"
                + "schema_name, \n"
                + "query_type, \n"
                + "session_id, \n"
                + "user_name, \n"
                + "warehouse_name, \n"
                + "cluster_number, \n"
                + "query_tag, \n"
                + "execution_status, \n"
                + "error_code, \n"
                + "error_message, \n"
                + "start_time, \n"
                + "end_time, \n"
                + "bytes_scanned, \n"
                + "percentage_scanned_from_cache, \n"
                + "bytes_written, \n"
                + "rows_produced, \n"
                + "rows_inserted, \n"
                + "rows_updated, \n"
                + "rows_deleted, \n"
                + "rows_unloaded, \n"
                + "bytes_deleted, \n"
                + "partitions_scanned, \n"
                + "partitions_total, \n"
                + "bytes_spilled_to_local_storage, \n"
                + "bytes_spilled_to_remote_storage, \n"
                + "bytes_sent_over_the_network, \n"
                + "total_elapsed_time, \n"
                + "compilation_time, \n"
                + "execution_time, \n"
                + "queued_provisioning_time, \n"
                + "queued_repair_time, \n"
                + "queued_overload_time, \n"
                + "transaction_blocked_time, \n"
                + "list_external_files_time, \n"
                + "credits_used_cloud_services, \n"
                + "query_load_percent, \n"
                + "query_acceleration_bytes_scanned, \n"
                + "query_acceleration_partitions_scanned, \n"
                + "child_queries_wait_time, \n"
                + "transaction_id \n"
                + "FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY\n"
                + "WHERE end_time >= to_timestamp_ltz('%s')\n"
                + "AND end_time <= to_timestamp_ltz('%s')\n"
                + "AND is_client_generated_statement = FALSE\n");
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
      queryBuilder
          .append("AND start_time >= ")
          .append(arguments.getQueryLogEarliestTimestamp())
          .append("\n");
    if (overrideWhere != null) queryBuilder.append(" AND ").append(overrideWhere);
    return queryBuilder.toString().replace('\n', ' ');
  }

  @CheckForNull
  protected String getOverrideQuery(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    String overrideQuery = arguments.getDefinition(SnowflakeLogConnectorProperties.OVERRIDE_QUERY);
    if (overrideQuery != null) {
      if (StringUtils.countMatches(overrideQuery, "%s") != 2)
        throw new MetadataDumperUsageException(
            "Custom query for log dump needs two \"%s\" expansions, they will be expanded to"
                + " end_time lower and upper boundaries.");
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
    Duration rotationDuration = arguments.getQueryLogRotationFrequency();
    ZonedIntervalIterable queryLogIntervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, rotationDuration, TimeTruncator.createBasedOnDuration(rotationDuration));
    LOG.info("Exporting query log for " + queryLogIntervals);

    if (!arguments.isAssessment()) {
      TaskDescription queryHistoryTask =
          new TaskDescription(ZIP_ENTRY_PREFIX, newQueryFormat(arguments), Header.class);
      queryLogIntervals.forEach(interval -> addJdbcTask(out, interval, queryHistoryTask));
      return;
    }

    TaskDescription queryHistoryTask =
        new TaskDescription(
            QueryHistoryExtendedFormat.ZIP_ENTRY_PREFIX,
            createExtendedQueryFromAccountUsage(arguments),
            QueryHistoryExtendedFormat.Header.class);
    queryLogIntervals.forEach(interval -> addJdbcTask(out, interval, queryHistoryTask));

    List<TaskDescription> timeSeriesTasks = createTimeSeriesTasks(arguments);
    Duration duration = Duration.ofDays(1);
    ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, duration, TimeTruncator.createBasedOnDuration(duration))
        .forEach(interval -> timeSeriesTasks.forEach(task -> addJdbcTask(out, interval, task)));
  }

  private static void addJdbcTask(
      List<? super Task<?>> out, ZonedInterval interval, TaskDescription task) {
    String query =
        String.format(
            task.unformattedQuery,
            SQL_FORMAT.format(interval.getStart()),
            SQL_FORMAT.format(interval.getEndInclusive()));
    String file =
        task.zipPrefix
            + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
            + ".csv";
    out.add(new JdbcSelectTask(file, query).withHeaderClass(task.headerClass));
  }

  private String getOverrideableQuery(
      @Nullable String overrideQuery, @Nonnull String defaultSql, @Nonnull String whereField) {
    String sql = overrideQuery != null ? overrideQuery : defaultSql;
    return sql
        + "\n"
        + "WHERE "
        + whereField
        + " >= to_timestamp_ltz('%s')\n"
        + "AND "
        + whereField
        + " <= to_timestamp_ltz('%s')";
  }

  private String parseColumnsFromHeader(Class<? extends Enum<?>> headerClass) {
    return Arrays.stream(headerClass.getEnumConstants())
        .map(Enum::name)
        .map(name -> CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name))
        .collect(Collectors.joining(", "));
  }

  private List<TaskDescription> createTimeSeriesTasks(ConnectorArguments arguments) {
    String queryPrefix = "SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.%s";
    return Arrays.asList(
        new TaskDescription(
            WarehouseEventsHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(WarehouseEventsHistoryFormat.Header.class),
                    "WAREHOUSE_EVENTS_HISTORY"),
                "TIMESTAMP"),
            WarehouseEventsHistoryFormat.Header.class),
        new TaskDescription(
            AutomaticClusteringHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(AutomaticClusteringHistoryFormat.Header.class),
                    "AUTOMATIC_CLUSTERING_HISTORY"),
                "END_TIME"),
            AutomaticClusteringHistoryFormat.Header.class),
        new TaskDescription(
            CopyHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.COPY_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(CopyHistoryFormat.Header.class),
                    "COPY_HISTORY"),
                "LAST_LOAD_TIME"),
            CopyHistoryFormat.Header.class),
        new TaskDescription(
            DatabaseReplicationUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties
                        .DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(DatabaseReplicationUsageHistoryFormat.Header.class),
                    "DATABASE_REPLICATION_USAGE_HISTORY"),
                "END_TIME"),
            DatabaseReplicationUsageHistoryFormat.Header.class),
        new TaskDescription(
            LoginHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.LOGIN_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(LoginHistoryFormat.Header.class),
                    "LOGIN_HISTORY"),
                "EVENT_TIMESTAMP"),
            LoginHistoryFormat.Header.class),
        new TaskDescription(
            MeteringDailyHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.METERING_DAILY_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(MeteringDailyHistoryFormat.Header.class),
                    "METERING_DAILY_HISTORY"),
                "USAGE_DATE"),
            MeteringDailyHistoryFormat.Header.class),
        new TaskDescription(
            PipeUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.PIPE_USAGE_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(PipeUsageHistoryFormat.Header.class),
                    "PIPE_USAGE_HISTORY"),
                "END_TIME"),
            PipeUsageHistoryFormat.Header.class),
        new TaskDescription(
            QueryAccelerationHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(QueryAccelerationHistoryFormat.Header.class),
                    "QUERY_ACCELERATION_HISTORY"),
                "END_TIME"),
            QueryAccelerationHistoryFormat.Header.class),
        new TaskDescription(
            ReplicationGroupUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(ReplicationGroupUsageHistoryFormat.Header.class),
                    "REPLICATION_GROUP_USAGE_HISTORY"),
                "END_TIME"),
            ReplicationGroupUsageHistoryFormat.Header.class),
        new TaskDescription(
            ServerlessTaskHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(ServerlessTaskHistoryFormat.Header.class),
                    "SERVERLESS_TASK_HISTORY"),
                "END_TIME"),
            ServerlessTaskHistoryFormat.Header.class),
        new TaskDescription(
            TaskHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.TASK_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(TaskHistoryFormat.Header.class),
                    "TASK_HISTORY"),
                "COMPLETED_TIME"),
            TaskHistoryFormat.Header.class),
        new TaskDescription(
            WarehouseLoadHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(WarehouseLoadHistoryFormat.Header.class),
                    "WAREHOUSE_LOAD_HISTORY"),
                "END_TIME"),
            WarehouseLoadHistoryFormat.Header.class),
        new TaskDescription(
            WarehouseMeteringHistoryFormat.ZIP_ENTRY_PREFIX,
            getOverrideableQuery(
                arguments.getDefinition(
                    SnowflakeLogConnectorProperties.WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY),
                String.format(
                    queryPrefix,
                    parseColumnsFromHeader(WarehouseMeteringHistoryFormat.Header.class),
                    "WAREHOUSE_METERING_HISTORY"),
                "END_TIME"),
            WarehouseMeteringHistoryFormat.Header.class));
  }
}
