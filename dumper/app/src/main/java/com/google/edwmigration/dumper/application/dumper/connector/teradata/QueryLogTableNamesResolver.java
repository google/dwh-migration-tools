/*
 * Copyright 2022-2024 Google LLC
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

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_ALTERNATES;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_QUERY_LOG_ALTERNATES_DEPRECATION_MESSAGE;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.DEF_LOG_TABLE;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.ASSESSMENT_DEF_LOG_TABLE;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.TeradataLogsConnectorProperty.QUERY_LOGS_TABLE;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.TeradataLogsConnectorProperty.SQL_LOGS_TABLE;
import static com.google.edwmigration.dumper.application.dumper.utils.OptionalUtils.optionallyIfNotEmpty;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolver that retrieves the names of query log tables from the command-line options.
 *
 * <p>Teradata saves query logs in two tables: {@code DBC.DBQLogTbl} and {@code DBC.DBQLSqlTbl}.
 * These two tables are called query log table and SQL log table for the purposes of the query logs
 * extraction.
 *
 * <p>Depending on the flags provided on the command-line, the {@code teradata-logs} connector uses
 * different pair of source tables for extraction:
 *
 * <ul>
 *   <li>If the {@code --assessment} flag is not used, then the logs are extracted from tables
 *       {@code DBC.DBQLogTbl} and {@code DBC.DBQLSqlTbl}
 *   <li>If the {@code --assessment} flag is used, then the logs are extracted from tables {@code
 *       DBC.QryLogV} (a view that selects the logs from {@code DBC.DBQLogTbl} table) and {@code
 *       DBC.DBQLSqlTbl}
 * </ul>
 *
 * <p>Additionally, the user can specify an alternative table for each of the two tables:
 *
 * <ul>
 *   <li>For the query log table, the command-line flag is: {@code -Dteradata-logs.query-log-table}
 *   <li>For the SQL log table, the command-line flag is: {@code -Dteradata-logs.sql-log-table}
 * </ul>
 *
 * <p>Deprecated: There is also a second way of specifying both alternative tables by using the
 * {@code --query-log-alternates} command-line flag that accepts these two log tables as a parameter
 * (separated by a comma).
 */
class QueryLogTableNamesResolver {
  private static final Logger LOG = LoggerFactory.getLogger(QueryLogTableNamesResolver.class);

  private static final String TERADATA_LOGS_CONNECTOR_NAME = "teradata-logs";

  static QueryLogTableNames resolve(ConnectorArguments arguments) {
    Preconditions.checkArgument(
        arguments.getConnectorName().equalsIgnoreCase(TERADATA_LOGS_CONNECTOR_NAME),
        "Resolver requires the connector to be '%s'.",
        TERADATA_LOGS_CONNECTOR_NAME);
    if (arguments.getQueryLogAlternates().isEmpty()) {
      return resolveFromDefinitionOptions(arguments);
    }
    return resolveFromLegacyOption(arguments);
  }

  private static QueryLogTableNames resolveFromDefinitionOptions(ConnectorArguments arguments) {
    boolean alternateQueryLogTableSpecified = arguments.isDefinitionSpecified(QUERY_LOGS_TABLE);
    boolean alternateSqlLogTableSpecified = arguments.isDefinitionSpecified(SQL_LOGS_TABLE);
    String queryLogTable =
        optionallyIfNotEmpty(arguments.getDefinition(QUERY_LOGS_TABLE))
            .orElse(arguments.isAssessment() ? ASSESSMENT_DEF_LOG_TABLE : DEF_LOG_TABLE);
    String sqlLogTable = arguments.getDefinitionOrDefault(SQL_LOGS_TABLE);
    if (alternateQueryLogTableSpecified && !alternateSqlLogTableSpecified) {
      LOG.warn(
          "The alternate query log table was provided using the '-D{}' flag, but no"
              + " alternate SQL table was provided using the '-D{}' flag. The following tables"
              + " will be joined to extract the query logs: '{}' and '{}'.",
          QUERY_LOGS_TABLE.getName(),
          SQL_LOGS_TABLE.getName(),
          queryLogTable,
          sqlLogTable);
    } else if (!alternateQueryLogTableSpecified && alternateSqlLogTableSpecified) {
      LOG.warn(
          "The alternate SQL log table was provided using the '-D{}' flag, but no alternate"
              + " query table was provided using the '-D{}' flag. The following tables will be"
              + " joined to extract the query logs: '{}' and '{}'.",
          SQL_LOGS_TABLE.getName(),
          QUERY_LOGS_TABLE.getName(),
          queryLogTable,
          sqlLogTable);
    }
    return QueryLogTableNames.create(
        queryLogTable,
        sqlLogTable,
        alternateQueryLogTableSpecified || alternateSqlLogTableSpecified);
  }

  private static final QueryLogTableNames resolveFromLegacyOption(ConnectorArguments arguments) {
    List<String> legacyAlternates = arguments.getQueryLogAlternates();
    boolean alternateQueryLogTableSpecified = arguments.isDefinitionSpecified(QUERY_LOGS_TABLE);
    boolean alternateSqlLogTableSpecified = arguments.isDefinitionSpecified(SQL_LOGS_TABLE);
    if (alternateQueryLogTableSpecified || alternateSqlLogTableSpecified) {
      ImmutableList.Builder<String> flagsUsed = ImmutableList.builder();
      flagsUsed.add(OPT_QUERY_LOG_ALTERNATES);
      if (alternateQueryLogTableSpecified) {
        flagsUsed.add(QUERY_LOGS_TABLE.getName());
      }
      if (alternateSqlLogTableSpecified) {
        flagsUsed.add(SQL_LOGS_TABLE.getName());
      }
      throw new MetadataDumperUsageException(
          String.format(
                  "Mixed alternate query log table configuration detected in flags '%s'. (",
                  flagsUsed.build())
              + OPT_QUERY_LOG_ALTERNATES_DEPRECATION_MESSAGE
              + ")");
    }
    if (legacyAlternates.size() != 2) {
      throw new MetadataDumperUsageException(
          "Alternate query log tables must be given as a pair separated by a comma;"
              + " you specified: '"
              + legacyAlternates
              + "'. ("
              + OPT_QUERY_LOG_ALTERNATES_DEPRECATION_MESSAGE
              + ")");
    }
    return QueryLogTableNames.create(
        /* queryLogsTableName= */ legacyAlternates.get(0),
        /* sqlLogsTableName= */ legacyAlternates.get(1),
        /* usingAtLeastOneAlternate= */ true);
  }
}
