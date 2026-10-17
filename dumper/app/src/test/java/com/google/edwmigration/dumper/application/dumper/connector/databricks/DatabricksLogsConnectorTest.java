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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.forbidden;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.ALL_WORKSPACES;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.SOURCE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WAREHOUSE_TRANSPORT;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WORKSPACE_ID;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.REDACTED_STATEMENT_TEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksLogsConnectorTest {

  private static final String URL = "https://workspace.cloud.databricks.com";
  private static final String WAREHOUSE = "wh-123";
  private static final String CLUSTER = "0123-456789-abcdefgh";
  private static final String WORKSPACE = "1234567890123456";
  private static final String OTHER_WORKSPACE = "2222222222222222";
  private static final String PERMISSION_ERROR = "[INSUFFICIENT_PERMISSIONS] No USE SCHEMA.";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final String TEXT_PROBE_END = " AND statement_text <> '' LIMIT 1";
  private static final String BLANK_TEXT_PROBE_END =
      " AND (statement_text IS NULL OR statement_text = '') LIMIT 1";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static WireMockServer server;

  private final DatabricksLogsConnector connector = new DatabricksLogsConnector();
  // The ID of the --url workspace, which the SDK reads from the host metadata of the workspace.
  // The test server has none, so openHandle sets the ID in the client configuration.
  private String urlWorkspaceId = WORKSPACE;

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  @Before
  public void resetServer() {
    server.resetAll();
  }

  @Test
  public void validate_checksSourceComputeTransportAndWorkspaceIdOption() {
    connector.validate(args(URL));
    connector.validate(args(URL, "--warehouse", WAREHOUSE, definition(WORKSPACE_ID, "1, 2")));
    connector.validate(args(URL, "--cluster", CLUSTER, definition(SOURCE, " System-Table ")));
    connector.validate(args(URL, "--warehouse", WAREHOUSE, definition(SOURCE, "api")));
    connector.validate(
        args(URL, "--warehouse", WAREHOUSE, definition(WAREHOUSE_TRANSPORT, " INLINE ")));

    assertValidateRejects("--assessment");
    assertThrows(MetadataDumperUsageException.class, () -> connector.validate(args(" ")));
    assertValidateRejects("--warehouse", WAREHOUSE, "--cluster", CLUSTER);
    assertValidateRejects("--cluster", CLUSTER, definition(WAREHOUSE_TRANSPORT, "inline"));
    assertTrue(assertValidateRejects(definition(SOURCE, "sql")).contains("'system-table'"));
    // The usage errors show how to choose where the queries are read from.
    assertTrue(
        assertValidateRejects(definition(SOURCE, "system-table"))
            .contains("--warehouse <warehouse-id>"));
    assertTrue(
        assertValidateRejects(definition(WORKSPACE_ID, WORKSPACE)).contains("--query-log-days"));
    assertTrue(
        assertValidateRejects(
                "--cluster", CLUSTER, definition(SOURCE, "api"), definition(WORKSPACE_ID, "all"))
            .contains(SOURCE.getName() + "=api"));
    assertTrue(
        assertValidateRejects("--cluster", CLUSTER, definition(WORKSPACE_ID, "1,,2"))
            .contains("'1,,2'"));
  }

  @Test
  public void open_resolvesWorkspaceIds() throws Exception {
    stubProbeSucceeded(ImmutableList.of(probeRow("SELECT 1")));
    // By default, the --url workspace.
    assertEquals(ImmutableList.of(WORKSPACE), openWarehouseHandle().workspaceIds());
    assertEquals(
        ImmutableList.of(WORKSPACE, OTHER_WORKSPACE),
        openWarehouseHandle(definition(WORKSPACE_ID, WORKSPACE + "," + OTHER_WORKSPACE))
            .workspaceIds());
    assertEquals(
        ImmutableList.of(),
        openWarehouseHandle(definition(WORKSPACE_ID, ALL_WORKSPACES)).workspaceIds());

    // Without the ID of the --url workspace, the query history API is read, unless the workspace
    // IDs are given or the system table is required.
    this.urlWorkspaceId = null;
    assertEquals(
        ImmutableList.of(OTHER_WORKSPACE),
        openWarehouseHandle(definition(WORKSPACE_ID, OTHER_WORKSPACE)).workspaceIds());
    assertNull(openWarehouseHandle().sqlRunner());
    assertNull(openHandle().sqlRunner());
    String usageError =
        assertThrows(
                MetadataDumperUsageException.class,
                () -> openWarehouseHandle(definition(SOURCE, "system-table")))
            .getMessage();
    assertTrue(usageError.contains(WORKSPACE_ID.getName()));
    assertTrue(usageError.contains("DATABRICKS_WORKSPACE_ID"));
  }

  @Test
  public void open_readsTheSystemTableOrTheApi() throws Exception {
    // Without a SQL warehouse or cluster, or with -Ddatabricks.logs.source=api, the API is read.
    // The system table of a range without queries is read: neither probe finds a query.
    stubProbeSucceeded(ImmutableList.of());
    stubBlankTextProbeSucceeded(ImmutableList.of());
    assertNull(openHandle().sqlRunner());
    assertNull(openWarehouseHandle(definition(SOURCE, "API")).sqlRunner());
    assertNotNull(openWarehouseHandle().sqlRunner());

    // The probe fails with an API error, such as a missing permission on the SQL warehouse.
    stubProbe(forbidden());
    assertNull(openWarehouseHandle().sqlRunner());

    // With -Ddatabricks.logs.source=system-table or -Ddatabricks.logs.workspace-id, the query
    // history API never replaces the system table.
    stubProbeSucceeded(ImmutableList.of(probeRow(REDACTED_STATEMENT_TEXT)));
    assertNull(openWarehouseHandle().sqlRunner());
    assertNotNull(openWarehouseHandle(definition(SOURCE, "system-table")).sqlRunner());
    assertNotNull(openWarehouseHandle(definition(WORKSPACE_ID, WORKSPACE)).sqlRunner());

    // The same holds when the range has queries but none has a statement text.
    stubProbeSucceeded(ImmutableList.of());
    stubBlankTextProbeSucceeded(ImmutableList.of(probeRow("")));
    assertNull(openWarehouseHandle().sqlRunner());
    assertNotNull(openWarehouseHandle(definition(SOURCE, "system-table")).sqlRunner());
    assertNotNull(openWarehouseHandle(definition(WORKSPACE_ID, WORKSPACE)).sqlRunner());

    stubProbe(
        okJson(
            "{\"status\":{\"state\":\"FAILED\",\"error\":{\"message\":\""
                + PERMISSION_ERROR
                + "\"}}}"));
    assertTrue(openFailure(definition(WORKSPACE_ID, ALL_WORKSPACES)).contains(PERMISSION_ERROR));
    String systemTableFailure = openFailure(definition(SOURCE, "system-table"));
    assertTrue(systemTableFailure.contains(PERMISSION_ERROR));
    assertTrue(systemTableFailure.contains("SELECT on system.query.history"));
  }

  @Test
  public void findIntervalsOlderThan_returnsTheIntervalsThatEndedBeforeTheRetention() {
    Duration retention = Duration.ofDays(30);
    ZonedDateTime now = ZonedDateTime.parse("2026-10-11T12:00:00Z");
    // The retention starts in the middle of this day, so the day still has queries.
    ZonedDateTime firstKeptDay = now.minus(retention).truncatedTo(ChronoUnit.DAYS);
    ZonedIntervalIterable days =
        ZonedIntervalIterableGenerator.forDateRangeWithIntervalDuration(
            firstKeptDay.minusDays(2), firstKeptDay.plusDays(2), Duration.ofDays(1));

    assertEquals(
        ImmutableList.of(
            new ZonedInterval(firstKeptDay.minusDays(2), firstKeptDay.minusDays(1)),
            new ZonedInterval(firstKeptDay.minusDays(1), firstKeptDay)),
        DatabricksLogsConnector.findIntervalsOlderThan(days, retention, now));
    // Two days earlier, the retention started in the first day, so no day is older.
    assertEquals(
        ImmutableList.of(),
        DatabricksLogsConnector.findIntervalsOlderThan(days, retention, now.minusDays(2)));
  }

  @Test
  public void addTasksTo_readsWholeDaysUnlessTheRotationFrequencyIsGiven() {
    // Each entry is named after the start of its interval.
    assertEquals(
        ImmutableList.of(
            "query_history_2026-10-09T000000Z.csv", "query_history_2026-10-10T000000Z.csv"),
        queryHistoryEntryNames(
            "--query-log-start", "2026-10-09 13:00:00", "--query-log-end", "2026-10-10 15:00:00"));
    assertEquals(
        ImmutableList.of(
            "query_history_2026-10-09T130000Z.csv", "query_history_2026-10-09T140000Z.csv"),
        queryHistoryEntryNames(
            "--query-log-start",
            "2026-10-09 13:00:00",
            "--query-log-end",
            "2026-10-09 15:00:00",
            "--query-log-rotation-frequency",
            "hourly"));
  }

  private List<String> queryHistoryEntryNames(String... extraArguments) {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, args(URL, extraArguments));
    List<String> entryNames = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (task instanceof QueryHistoryTask) {
        entryNames.add(task.getTargetPath());
      }
    }

    return entryNames;
  }

  private String assertValidateRejects(String... extraArguments) {
    return assertThrows(
            MetadataDumperUsageException.class, () -> connector.validate(args(URL, extraArguments)))
        .getMessage();
  }

  private String openFailure(String... extraArguments) {
    return assertThrows(IOException.class, () -> openWarehouseHandle(extraArguments)).getMessage();
  }

  private DatabricksLogsHandle openWarehouseHandle(String... extraArguments) throws IOException {
    return openHandle(
        ObjectArrays.concat(
            new String[] {"--warehouse", WAREHOUSE, definition(WAREHOUSE_TRANSPORT, "inline")},
            extraArguments,
            String.class));
  }

  /** Opens and closes the handle of a run, which keeps what the run resolved. */
  private DatabricksLogsHandle openHandle(String... extraArguments) throws IOException {
    ConnectorArguments arguments = args(server.baseUrl(), extraArguments);
    WorkspaceClient client = AbstractDatabricksConnector.createWorkspaceClient(arguments);
    client.config().setWorkspaceId(urlWorkspaceId);
    try (Handle handle = connector.open(arguments, client)) {
      return (DatabricksLogsHandle) handle;
    }
  }

  // The first probe reads one query that has a statement text.
  private static void stubProbeSucceeded(Object rows) throws IOException {
    stubProbe(succeeded(rows));
  }

  private static void stubProbe(ResponseDefinitionBuilder response) {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", containing(TEXT_PROBE_END)))
            .willReturn(response));
  }

  // The second probe, which runs when the first one finds no query, looks for a query without text.
  private static void stubBlankTextProbeSucceeded(Object rows) throws IOException {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", containing(BLANK_TEXT_PROBE_END)))
            .willReturn(succeeded(rows)));
  }

  private static ResponseDefinitionBuilder succeeded(Object rows) throws IOException {
    return okJson(
        "{\"status\":{\"state\":\"SUCCEEDED\"},\"result\":{\"data_array\":"
            + MAPPER.writeValueAsString(rows)
            + "}}");
  }

  // The probes read only the statement text, which is the sixth column.
  private static ImmutableList<String> probeRow(String statementText) {
    return ImmutableList.of("", WORKSPACE, "stmt-1", "", "", statementText);
  }

  private static String definition(DatabricksLogsProperty property, String value) {
    return "-D" + property.getName() + "=" + value;
  }

  private static ConnectorArguments args(String url, String... extraArguments) {
    return ConnectorArguments.create(
        ImmutableList.<String>builder()
            .add("--connector", DatabricksLogsConnector.CONNECTOR_NAME, "--url", url)
            .add("--password", "test-token")
            .add(extraArguments)
            .build());
  }
}
