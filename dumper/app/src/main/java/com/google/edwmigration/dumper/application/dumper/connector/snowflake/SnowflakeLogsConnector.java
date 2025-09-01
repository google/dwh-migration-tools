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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.SCHEMA_ONLY_SOURCE;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;

import com.google.auto.service.AutoService;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabaseForConnection;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.IntervalExpander;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.SnowflakeLogsDumpFormat;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@AutoService(Connector.class)
@RespectsArgumentAssessment
@RespectsArgumentDatabaseForConnection
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class SnowflakeLogsConnector extends AbstractSnowflakeConnector
    implements LogsConnector, SnowflakeLogsDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger logger = LoggerFactory.getLogger(SnowflakeLogsConnector.class);

  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  private final SnowflakeInput inputSource;

  SnowflakeLogsConnector(@Nonnull String name, @Nonnull SnowflakeInput inputSource) {
    super(name);
    this.inputSource = inputSource;
  }

  public SnowflakeLogsConnector() {
    this("snowflake-logs", SCHEMA_ONLY_SOURCE);
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
    private final String override;
    private final Class<? extends Enum<?>> headerClass;
    private final String view;
    private final String whereField;

    private final TaskCategory taskCategory;

    private TaskDescription(
        String zipPrefix,
        String override,
        Class<? extends Enum<?>> headerClass,
        String view,
        String whereField) {
      this.zipPrefix = zipPrefix;
      this.override = override;
      this.headerClass = headerClass;
      this.view = view;
      this.whereField = whereField;
      this.taskCategory = TaskCategory.REQUIRED;
    }

    private TaskDescription(
        String zipPrefix,
        String override,
        Class<? extends Enum<?>> headerClass,
        String view,
        String whereField,
        TaskCategory taskCategory) {
      this.zipPrefix = zipPrefix;
      this.override = override;
      this.headerClass = headerClass;
      this.view = view;
      this.whereField = whereField;
      this.taskCategory = taskCategory;
    }
  }

  @Override
  protected final void validateForConnector(@Nonnull ConnectorArguments arguments) {
    if (arguments.isAssessment() && arguments.hasQueryLogEarliestTimestamp()) {
      throw unsupportedOption(ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP);
    }
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return SnowflakeLogConnectorProperties.class;
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Dumps logs from Snowflake.";
  }

  @Nonnull
  private BinaryOperator<String> newQueryFormat(@Nonnull ConnectorArguments arguments) {
    // Docref: https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html
    // Per the docref, Snowflake only retains/returns seven trailing days of logs.
    switch (inputSource) {
      case USAGE_THEN_SCHEMA_SOURCE:
        throw new IllegalArgumentException("Unsupported input source for Snowflake logs.");
      case USAGE_ONLY_SOURCE:
        return createQueryFromAccountUsage(arguments);
      case SCHEMA_ONLY_SOURCE:
        return createQueryFromInformationSchema(arguments);
    }
    throw new AssertionError();
  }

  @Nonnull
  private BinaryOperator<String> createQueryFromAccountUsage(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    return (startTime, endTime) -> {
      String overrideQuery = getOverrideQuery(arguments);
      if (overrideQuery != null) return overrideQuery;

      String overrideWhere = getOverrideWhere(arguments);

      StringBuilder queryBuilder = new StringBuilder(simpleQuery(startTime, endTime));
      if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
        queryBuilder
            .append("AND start_time >= ")
            .append(arguments.getQueryLogEarliestTimestamp())
            .append("\n");
      if (overrideWhere != null) queryBuilder.append(" AND ").append(overrideWhere);
      return queryBuilder.toString().replace('\n', ' ');
    };
  }

  @Nonnull
  private BinaryOperator<String> createQueryFromInformationSchema(ConnectorArguments arguments)
      throws MetadataDumperUsageException {

    return (startTime, endTime) -> {
      // Docref:
      // https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html
      // Per the docref, Snowflake only retains/returns seven trailing days of logs in
      // INFORMATION_SCHEMA.
      String overrideQuery = getOverrideQuery(arguments);
      if (overrideQuery != null) return overrideQuery;

      String overrideWhere = getOverrideWhere(arguments);

      StringBuilder queryBuilder = new StringBuilder(informationSchemaQuery(startTime, endTime));
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
    };
  }

  @Nonnull
  private BinaryOperator<String> createExtendedQueryFromAccountUsage(
      @Nonnull ConnectorArguments arguments) {
    return (startTime, endTime) -> {
      String overrideQuery = getOverrideQuery(arguments);
      if (overrideQuery != null) return overrideQuery;

      String overrideWhere = getOverrideWhere(arguments);

      StringBuilder queryBuilder = new StringBuilder(extendedQuery(startTime, endTime));

      if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
        queryBuilder
            .append("AND start_time >= ")
            .append(arguments.getQueryLogEarliestTimestamp())
            .append("\n");
      if (overrideWhere != null) queryBuilder.append(" AND ").append(overrideWhere);
      return queryBuilder.toString().replace('\n', ' ');
    };
  }

  @CheckForNull
  private String getOverrideQuery(@Nonnull ConnectorArguments arguments)
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
  private String getOverrideWhere(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    return arguments.getDefinition(SnowflakeLogConnectorProperties.OVERRIDE_WHERE);
  }

  @Override
  public final void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
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
            arguments, rotationDuration, IntervalExpander.createBasedOnDuration(rotationDuration));
    logger.info("Exporting query log for " + queryLogIntervals);

    if (!arguments.isAssessment()) {
      BinaryOperator<String> format = newQueryFormat(arguments);
      for (ZonedInterval interval : queryLogIntervals) {
        String prefix = SnowflakeLogsDumpFormat.ZIP_ENTRY_PREFIX;
        Class<SnowflakeLogsDumpFormat.Header> header = SnowflakeLogsDumpFormat.Header.class;

        out.add(makeJdbcTask(format, interval, prefix, header, TaskCategory.REQUIRED));
      }
      return;
    }

    BinaryOperator<String> queryFormat = createExtendedQueryFromAccountUsage(arguments);
    for (ZonedInterval interval : queryLogIntervals) {
      String prefix = QueryHistoryExtendedFormat.ZIP_ENTRY_PREFIX;
      Class<QueryHistoryExtendedFormat.Header> header = QueryHistoryExtendedFormat.Header.class;

      out.add(makeJdbcTask(queryFormat, interval, prefix, header, TaskCategory.REQUIRED));
    }

    List<TaskDescription> timeSeriesTasks = createTimeSeriesTasks(arguments);
    IntervalExpander expander = IntervalExpander.createBasedOnDuration(Duration.ofDays(1));
    for (ZonedInterval interval : getIntervals(arguments, expander)) {
      for (TaskDescription description : timeSeriesTasks) {

        BinaryOperator<String> format =
            getOverrideableQuery(
                description.override,
                description.headerClass,
                description.view,
                description.whereField);

        out.add(
            makeJdbcTask(
                format,
                interval,
                description.zipPrefix,
                description.headerClass,
                description.taskCategory));
      }
    }
  }

  @Nonnull
  private static Iterable<ZonedInterval> getIntervals(
      @Nonnull ConnectorArguments arguments, @Nonnull IntervalExpander expander) {
    return ZonedIntervalIterableGenerator.forConnectorArguments(
        arguments, expander.duration(), expander);
  }

  @Nonnull
  private static Task<?> makeJdbcTask(
      @Nonnull BinaryOperator<String> queryFormat,
      @Nonnull ZonedInterval interval,
      @Nonnull String zipPrefix,
      @Nonnull Class<? extends Enum<?>> headerClass,
      @Nonnull TaskCategory category) {
    String startTime = SQL_FORMAT.format(interval.getStart());
    String endTime = SQL_FORMAT.format(interval.getEndInclusive());

    String query = queryFormat.apply(startTime, endTime);
    String file = getEntryFileNameWithTimestamp(zipPrefix, interval);
    return new JdbcSelectTask(file, query, category).withHeaderClass(headerClass);
  }

  private static MetadataDumperUsageException unsupportedOption(String option) {
    String assessment = ConnectorArguments.OPT_ASSESSMENT;
    String message =
        String.format("Unsupported option used with --%s: please remove --%s", assessment, option);
    return new MetadataDumperUsageException(message);
  }

  @Nonnull
  private static BinaryOperator<String> getOverrideableQuery(
      @Nullable String overrideQuery,
      @Nonnull Class<? extends Enum<?>> header,
      @Nonnull String view,
      @Nonnull String whereField) {
    return (startTime, endTime) -> {
      String columns = parseColumnsFromHeader(header);
      String sql = String.format("SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.%s", columns, view);
      if (overrideQuery != null) {
        sql = overrideQuery;
      }
      String startCondition = String.format("%s >= to_timestamp_ltz('%s')", whereField, startTime);
      String endCondition = String.format("%s <= to_timestamp_ltz('%s')", whereField, endTime);
      return sql + String.format("\nWHERE %s\nAND %s", startCondition, endCondition);
    };
  }

  private static String parseColumnsFromHeader(Class<? extends Enum<?>> headerClass) {
    return Arrays.stream(headerClass.getEnumConstants())
        .map(Enum::name)
        .map(name -> CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name))
        .collect(Collectors.joining(", "));
  }

  private ImmutableList<TaskDescription> createTimeSeriesTasks(
      @Nonnull ConnectorArguments arguments) {
    return ImmutableList.of(
        new TaskDescription(
            WarehouseEventsHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY),
            WarehouseEventsHistoryFormat.Header.class,
            "WAREHOUSE_EVENTS_HISTORY",
            "TIMESTAMP"),
        new TaskDescription(
            AutomaticClusteringHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY),
            AutomaticClusteringHistoryFormat.Header.class,
            "AUTOMATIC_CLUSTERING_HISTORY",
            "END_TIME"),
        new TaskDescription(
            CopyHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(SnowflakeLogConnectorProperties.COPY_HISTORY_OVERRIDE_QUERY),
            CopyHistoryFormat.Header.class,
            "COPY_HISTORY",
            "LAST_LOAD_TIME"),
        new TaskDescription(
            DatabaseReplicationUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY),
            DatabaseReplicationUsageHistoryFormat.Header.class,
            "DATABASE_REPLICATION_USAGE_HISTORY",
            "END_TIME"),
        new TaskDescription(
            LoginHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(SnowflakeLogConnectorProperties.LOGIN_HISTORY_OVERRIDE_QUERY),
            LoginHistoryFormat.Header.class,
            "LOGIN_HISTORY",
            "EVENT_TIMESTAMP"),
        new TaskDescription(
            MeteringDailyHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.METERING_DAILY_HISTORY_OVERRIDE_QUERY),
            MeteringDailyHistoryFormat.Header.class,
            "METERING_DAILY_HISTORY",
            "USAGE_DATE"),
        new TaskDescription(
            PipeUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.PIPE_USAGE_HISTORY_OVERRIDE_QUERY),
            PipeUsageHistoryFormat.Header.class,
            "PIPE_USAGE_HISTORY",
            "END_TIME"),
        new TaskDescription(
            QueryAccelerationHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY),
            QueryAccelerationHistoryFormat.Header.class,
            "QUERY_ACCELERATION_HISTORY",
            "END_TIME",
            TaskCategory.OPTIONAL),
        new TaskDescription(
            ReplicationGroupUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY),
            ReplicationGroupUsageHistoryFormat.Header.class,
            "REPLICATION_GROUP_USAGE_HISTORY",
            "END_TIME"),
        new TaskDescription(
            ServerlessTaskHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY),
            ServerlessTaskHistoryFormat.Header.class,
            "SERVERLESS_TASK_HISTORY",
            "END_TIME"),
        new TaskDescription(
            TaskHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(SnowflakeLogConnectorProperties.TASK_HISTORY_OVERRIDE_QUERY),
            TaskHistoryFormat.Header.class,
            "TASK_HISTORY",
            "COMPLETED_TIME"),
        new TaskDescription(
            WarehouseLoadHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY),
            WarehouseLoadHistoryFormat.Header.class,
            "WAREHOUSE_LOAD_HISTORY",
            "END_TIME"),
        new TaskDescription(
            WarehouseMeteringHistoryFormat.ZIP_ENTRY_PREFIX,
            arguments.getDefinition(
                SnowflakeLogConnectorProperties.WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY),
            WarehouseMeteringHistoryFormat.Header.class,
            "WAREHOUSE_METERING_HISTORY",
            "END_TIME"));
  }

  @Nonnull
  private static String simpleQuery(@Nonnull String startTime, @Nonnull String endTime) {
    return String.format(
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
            + "AND end_time <= to_timestamp_ltz('%s')\n",
        startTime, endTime);
  }

  @Nonnull
  private static String informationSchemaQuery(@Nonnull String startTime, @Nonnull String endTime) {
    return String.format(
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
            + "))\n",
        startTime, endTime);
  }

  @Nonnull
  private static String extendedQuery(@Nonnull String startTime, @Nonnull String endTime) {
    return String.format(
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
            + "AND is_client_generated_statement = FALSE\n",
        startTime, endTime);
  }
}
