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
import static java.util.Arrays.stream;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import javax.annotation.Nonnull;

enum SnowflakeLogsConnectorProperty implements ConnectorProperty {
  /* Basic overrides. */
  OVERRIDE_QUERY("snowflake.logs.query", "Custom query for log dump."),
  OVERRIDE_WHERE("snowflake.logs.where", "Custom where condition to append to query for log dump."),

  /* Time series custom queries. */
  WAREHOUSE_EVENTS_HISTORY_OVERRIDE_QUERY(
      "snowflake.warehouse_events_history.query", "Custom query for warehouse events history dump"),
  AUTOMATIC_CLUSTERING_HISTORY_OVERRIDE_QUERY(
      "snowflake.automatic_clustering_history.query",
      "Custom query for automatic clustering history dump"),
  COPY_HISTORY_OVERRIDE_QUERY("snowflake.copy_history.query", "Custom query for copy history dump"),
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
  QUERY_ATTRIBUTION_HISTORY_OVERRIDE_QUERY(
      "snowflake.query_attribution_history.query",
      "Custom query for query attribution history dump"),
  REPLICATION_GROUP_USAGE_HISTORY_OVERRIDE_QUERY(
      "snowflake.replication_group_usage_history.query",
      "Custom query for replication group usage history dump"),
  SERVERLESS_TASK_HISTORY_OVERRIDE_QUERY(
      "snowflake.serverless_task_history.query", "Custom query for serverless task history dump"),
  TASK_HISTORY_OVERRIDE_QUERY("snowflake.task_history.query", "Custom query for task history dump"),
  WAREHOUSE_LOAD_HISTORY_OVERRIDE_QUERY(
      "snowflake.warehouse_load_history.query", "Custom query for warehouse load history dump"),
  WAREHOUSE_METERING_HISTORY_OVERRIDE_QUERY(
      "snowflake.warehouse_metering_history.query",
      "Custom query for warehouse metering history dump");

  private final String name;
  private final String description;

  SnowflakeLogsConnectorProperty(String name, String description) {
    this.name = name;
    this.description = description;
  }

  static ImmutableList<ConnectorProperty> getConstants() {
    return stream(values()).collect(toImmutableList());
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
