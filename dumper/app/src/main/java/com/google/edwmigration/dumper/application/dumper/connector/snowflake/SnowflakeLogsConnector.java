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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.connector.snowflake.SnowflakeInput.SCHEMA_ONLY_SOURCE;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import com.google.auto.service.AutoService;
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
import java.util.List;
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

  enum SnowflakeLogConnectorProperties implements ConnectorProperty {
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

    private final TaskCategory taskCategory;

    private TaskDescription(
        String zipPrefix, String unformattedQuery, Class<? extends Enum<?>> headerClass) {
      this(zipPrefix, unformattedQuery, headerClass, TaskCategory.REQUIRED);
    }

    private TaskDescription(
        String zipPrefix,
        String unformattedQuery,
        Class<? extends Enum<?>> headerClass,
        TaskCategory taskCategory) {
      this.zipPrefix = zipPrefix;
      this.unformattedQuery = unformattedQuery;
      this.headerClass = headerClass;
      this.taskCategory = taskCategory;
    }
  }

  @Override
  protected final void validateForConnector(@Nonnull ConnectorArguments arguments) {
    if (arguments.isAssessment() && arguments.hasQueryLogEarliestTimestamp()) {
      throw unsupportedOption(ConnectorArguments.OPT_QUERY_LOG_EARLIEST_TIMESTAMP);
    }
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Dumps logs from Snowflake.";
  }

  @Override
  @Nonnull
  public ImmutableList<ConnectorProperty> getPropertyConstants() {
    return stream(SnowflakeLogConnectorProperties.values()).collect(toImmutableList());
  }

  private String newQueryFormat(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
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

  private String createQueryFromAccountUsage(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    @SuppressWarnings("OrphanedFormatString")
    String baseQuery =
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
            + "AND end_time <= to_timestamp_ltz('%s')\n";
    return addOverridesToQuery(arguments, baseQuery).replace('\n', ' ');
  }

  private String createQueryFromInformationSchema(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    // Docref: https://docs.snowflake.net/manuals/sql-reference/functions/query_history.html
    // Per the docref, Snowflake only retains/returns seven trailing days of logs in
    // INFORMATION_SCHEMA.
    @SuppressWarnings("OrphanedFormatString")
    String baseQuery =
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
            // It makes later formatting easier if we always have a 'WHERE'.
            + ")) WHERE 1=1\n";
    return addOverridesToQuery(arguments, baseQuery).replace('\n', ' ');
  }

  private String createExtendedQueryFromAccountUsage(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    @SuppressWarnings("OrphanedFormatString")
    String baseQuery =
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
            + "AND is_client_generated_statement = FALSE\n";
    return addOverridesToQuery(arguments, baseQuery).replace('\n', ' ');
  }

  @Nonnull
  static String addOverridesToQuery(
      @Nonnull ConnectorArguments arguments, @Nonnull String baseQuery) {
    String overrideQuery = getOverrideQuery(arguments);

    if (overrideQuery != null) {
      return overrideQuery;
    }
    StringBuilder queryBuilder = new StringBuilder(baseQuery);
    queryBuilder.append(earliestTimestamp(arguments));

    String overrideWhere = arguments.getDefinition(SnowflakeLogConnectorProperties.OVERRIDE_WHERE);
    if (overrideWhere != null) {
      queryBuilder.append(" AND " + overrideWhere);
    }
    return queryBuilder.toString();
  }

  @Nonnull
  static String earliestTimestamp(@Nonnull ConnectorArguments arguments) {
    String timestamp = arguments.getQueryLogEarliestTimestamp();
    if (isBlank(timestamp)) {
      return "";
    } else {
      return String.format("AND start_time >= %s\n", timestamp);
    }
  }

  @CheckForNull
  private static String getOverrideQuery(@Nonnull ConnectorArguments arguments)
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

    List<TaskDescription> timeSeriesTasks =
        TimeSeriesView.valuesInOrder.stream()
            .map(item -> item.taskDescription(arguments))
            .collect(toImmutableList());
    Duration duration = Duration.ofDays(1);
    ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, duration, IntervalExpander.createBasedOnDuration(duration))
        .forEach(interval -> timeSeriesTasks.forEach(task -> addJdbcTask(out, interval, task)));
  }

  private static void addJdbcTask(
      List<? super Task<?>> out, ZonedInterval interval, TaskDescription task) {
    String query =
        String.format(
            task.unformattedQuery,
            SQL_FORMAT.format(interval.getStart()),
            SQL_FORMAT.format(interval.getEndInclusive()));

    String file = getEntryFileNameWithTimestamp(task.zipPrefix, interval);
    out.add(new JdbcSelectTask(file, query, task.taskCategory).withHeaderClass(task.headerClass));
  }

  private static MetadataDumperUsageException unsupportedOption(String option) {
    String assessment = ConnectorArguments.OPT_ASSESSMENT;
    String message =
        String.format("Unsupported option used with --%s: please remove --%s", assessment, option);
    return new MetadataDumperUsageException(message);
  }

  static String overrideableQuery(
      @Nullable String override, @Nonnull String defaultSql, @Nonnull String whereField) {
    String start = whereField + " >= to_timestamp_ltz('%s')";
    String end = whereField + " <= to_timestamp_ltz('%s')";

    if (override != null) {
      return String.format("%s\nWHERE %s\nAND %s", override, start, end);
    } else {
      return String.format("%s\nWHERE %s\nAND %s", defaultSql, start, end);
    }
  }

  enum TimeSeriesView {
    WAREHOUSE_EVENTS_HISTORY(WarehouseEventsHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            WarehouseEventsHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "TIMESTAMP"),
            headerClass);
      }
    },
    AUTOMATIC_CLUSTERING_HISTORY(AutomaticClusteringHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            AutomaticClusteringHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    },
    COPY_HISTORY(CopyHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property = SnowflakeLogConnectorProperties.COPY_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            CopyHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "LAST_LOAD_TIME"),
            headerClass);
      }
    },
    DATABASE_REPLICATION_USAGE_HISTORY(DatabaseReplicationUsageHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.DATABASE_REPLICATION_USAGE_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            DatabaseReplicationUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    },
    LOGIN_HISTORY(LoginHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property = SnowflakeLogConnectorProperties.LOGIN_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            LoginHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "EVENT_TIMESTAMP"),
            headerClass);
      }
    },
    METERING_DAILY_HISTORY(MeteringDailyHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.METERING_DAILY_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            MeteringDailyHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "USAGE_DATE"),
            headerClass);
      }
    },
    PIPE_USAGE_HISTORY(PipeUsageHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.PIPE_USAGE_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            PipeUsageHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    },
    QUERY_ACCELERATION_HISTORY(QueryAccelerationHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.QUERY_ACCELERATION_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            QueryAccelerationHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass,
            TaskCategory.OPTIONAL);
      }
    },
    REPLICATION_GROUP_USAGE_HISTORY(ReplicationGroupUsageHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            QueryAccelerationHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass,
            TaskCategory.OPTIONAL);
      }
    },
    SERVERLESS_TASK_HISTORY(ServerlessTaskHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            ServerlessTaskHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    },
    TASK_HISTORY(TaskHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property = SnowflakeLogConnectorProperties.TASK_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            TaskHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "COMPLETED_TIME"),
            headerClass);
      }
    },
    WAREHOUSE_LOAD_HISTORY(WarehouseLoadHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            WarehouseLoadHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    },
    WAREHOUSE_METERING_HISTORY(WarehouseMeteringHistoryFormat.Header.class) {
      @Override
      TaskDescription taskDescription(@Nonnull ConnectorArguments arguments) {
        ConnectorProperty property =
            SnowflakeLogConnectorProperties.WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY;
        return new TaskDescription(
            WarehouseMeteringHistoryFormat.ZIP_ENTRY_PREFIX,
            overrideableQuery(arguments.getDefinition(property), queryPrefix, "END_TIME"),
            headerClass);
      }
    };

    final Class<? extends Enum<?>> headerClass;
    final String queryPrefix;

    TimeSeriesView(Class<? extends Enum<?>> headerClass) {
      this.headerClass = headerClass;
      this.queryPrefix = formatPrefix(headerClass, name());
    }

    static final ImmutableList<TimeSeriesView> valuesInOrder =
        ImmutableList.of(
            WAREHOUSE_EVENTS_HISTORY,
            AUTOMATIC_CLUSTERING_HISTORY,
            COPY_HISTORY,
            DATABASE_REPLICATION_USAGE_HISTORY,
            LOGIN_HISTORY,
            METERING_DAILY_HISTORY,
            PIPE_USAGE_HISTORY,
            QUERY_ACCELERATION_HISTORY,
            REPLICATION_GROUP_USAGE_HISTORY,
            SERVERLESS_TASK_HISTORY,
            TASK_HISTORY,
            WAREHOUSE_LOAD_HISTORY,
            WAREHOUSE_METERING_HISTORY);

    abstract TaskDescription taskDescription(@Nonnull ConnectorArguments arguments);
  }

  @Nonnull
  static String formatPrefix(@Nonnull Class<? extends Enum<?>> enumClass, @Nonnull String view) {
    String selectList =
        stream(enumClass.getEnumConstants())
            .map(AbstractSnowflakeConnector::columnOf)
            .collect(joining(", "));
    return String.format("SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.%s", selectList, view);
  }
}
