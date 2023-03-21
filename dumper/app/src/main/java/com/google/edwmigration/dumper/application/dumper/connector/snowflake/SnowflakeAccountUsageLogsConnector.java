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
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import org.apache.commons.lang3.StringUtils;

/** @author shevek */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Snowflake, using ACCOUNT_USAGE only.")
public class SnowflakeAccountUsageLogsConnector extends SnowflakeLogsConnector {

  public SnowflakeAccountUsageLogsConnector() {
    super("snowflake-account-usage-logs");
  }

  @Override
  protected String newQueryFormat(ConnectorArguments arguments)
      throws MetadataDumperUsageException {
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
                + "FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY\n"
                + "WHERE end_time >= to_timestamp_ltz('%s')\n"
                + "AND end_time <= to_timestamp_ltz('%s')\n");
    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    // TODO: Use ZonedIntervalIterable.forConnectorArguments()
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
      queryBuilder
          .append("AND start_time >= ")
          .append(arguments.getQueryLogEarliestTimestamp())
          .append("\n");
    if (overrideWhere != null) queryBuilder.append(" AND ").append(overrideWhere);
    return queryBuilder.toString().replace('\n', ' ');
  }
}
