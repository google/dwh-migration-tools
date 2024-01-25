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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import joptsimple.OptionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryLogTableNamesResolverTest {

  @Test
  public void resolve_success() throws IOException {
    ConnectorArguments args = new ConnectorArguments("--connector", "teradata-logs");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("dbc.DBQLogTbl", tableNames.queryLogsTableName());
    assertEquals("dbc.DBQLSQLTbl", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_withAssessmentFlag_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments("--connector", "teradata-logs", "--assessment");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("dbc.QryLogV", tableNames.queryLogsTableName());
    assertEquals("dbc.DBQLSQLTbl", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_legacyAlternateTables_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector",
            "teradata-logs",
            "--query-log-alternates",
            "SampleLogTable,SampleSqlTable");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("SampleLogTable", tableNames.queryLogsTableName());
    assertEquals("SampleSqlTable", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_alternateQueryLogsTable_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "teradata-logs", "-Dteradata-logs.query-logs-table=SampleTable");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("SampleTable", tableNames.queryLogsTableName());
    assertEquals("dbc.DBQLSQLTbl", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_alternateSqlLogsTable_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "teradata-logs", "-Dteradata-logs.sql-logs-table=SampleTable");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("dbc.DBQLogTbl", tableNames.queryLogsTableName());
    assertEquals("SampleTable", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_alternateSqlLogsTableForAssessment_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector",
            "teradata-logs",
            "--assessment",
            "-Dteradata-logs.sql-logs-table=SampleTable");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("dbc.QryLogV", tableNames.queryLogsTableName());
    assertEquals("SampleTable", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_alternateQueryLogsTables_success() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector",
            "teradata-logs",
            "-Dteradata-logs.query-logs-table=SampleLogTable",
            "-Dteradata-logs.sql-logs-table=SampleSqlTable");

    // Act
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(args);

    // Assert
    assertEquals("SampleLogTable", tableNames.queryLogsTableName());
    assertEquals("SampleSqlTable", tableNames.sqlLogsTableName());
  }

  @Test
  public void resolve_legacyAlternateTablesNoArgument_throwsException() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments("--connector", "teradata-logs", "--query-log-alternates");

    // Act
    OptionException e =
        assertThrows(OptionException.class, () -> QueryLogTableNamesResolver.resolve(args));

    // Assert
    assertEquals("Option query-log-alternates requires an argument", e.getMessage());
  }

  @Test
  public void resolve_legacyAlternateTablesOnlyOneTableInsteadOfPair_throwsException()
      throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector", "teradata-logs", "--query-log-alternates", "SampleTable");

    // Act
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> QueryLogTableNamesResolver.resolve(args));

    // Assert
    assertEquals(
        "Alternate query log tables must be given as a pair separated by a comma; you"
            + " specified: '[SampleTable]'. (The query-log-alternates option is deprecated, please use"
            + " -Dteradata-logs.query-log-table and -Dteradata-logs.sql-log-table instead)",
        e.getMessage());
  }

  @Test
  public void resolve_mixedLegacyAlternateTablesAndNewFlag_throwsException() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector",
            "teradata-logs",
            "--query-log-alternates",
            "SampleTable",
            "-Dteradata-logs.query-logs-table=SampleTable");

    // Act
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> QueryLogTableNamesResolver.resolve(args));

    // Assert
    assertEquals(
        "Mixed alternate query log table configuration detected in flags"
            + " '[query-log-alternates, teradata-logs.query-logs-table]'. (The query-log-alternates"
            + " option is deprecated, please use -Dteradata-logs.query-log-table and"
            + " -Dteradata-logs.sql-log-table instead)",
        e.getMessage());
  }

  @Test
  public void resolve_mixedLegacyAlternateTablesAndNewFlags_throwsException() throws IOException {
    ConnectorArguments args =
        new ConnectorArguments(
            "--connector",
            "teradata-logs",
            "--query-log-alternates",
            "SampleTable",
            "-Dteradata-logs.query-logs-table=SampleTable",
            "-Dteradata-logs.sql-logs-table=SampleTable");

    // Act
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> QueryLogTableNamesResolver.resolve(args));

    // Assert
    assertEquals(
        "Mixed alternate query log table configuration detected in flags"
            + " '[query-log-alternates, teradata-logs.query-logs-table, teradata-logs.sql-logs-table]'."
            + " (The query-log-alternates option is deprecated, please use"
            + " -Dteradata-logs.query-log-table and -Dteradata-logs.sql-log-table instead)",
        e.getMessage());
  }

  @Test
  public void resolve_unsupportedConnector_throwsException() throws IOException {
    ConnectorArguments args = new ConnectorArguments("--connector", "test-connector");

    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> QueryLogTableNamesResolver.resolve(args));

    // Assert
    assertEquals("Resolver requires the connector to be 'teradata-logs'.", e.getMessage());
  }
}
