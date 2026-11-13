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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.logs;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.task.AbstractTask.FORMAT;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;
import static com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat.ZIP_ENTRY_PREFIX;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.Disposition;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat.Header;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryHistoryTaskTest {

  private static final String ACCOUNT = "account-1";
  private static final String WORKSPACE = "1234567890123456";
  private static final ZonedInterval INTERVAL =
      new ZonedInterval(
          ZonedDateTime.parse("2026-10-09T13:00:00Z"), ZonedDateTime.parse("2026-10-09T14:00:00Z"));
  private static final long START_TIME_MILLIS = INTERVAL.getStart().toInstant().toEpochMilli();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static WireMockServer server;

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
  public void systemTable_writesExpectedCsv() throws Exception {
    // system.query.history has queries without an end time.
    List<String> rowWithoutEndTime = new ArrayList<>(systemTableRow("stmt-2", "SELECT 2"));
    rowWithoutEndTime.set(Header.EndTime.ordinal(), null);
    stubStatement(
        QueryHistoryTask.systemTableSql(INTERVAL, ImmutableList.of(WORKSPACE)),
        ImmutableList.of(
            systemTableRow("stmt-1", "SELECT * FROM sales.orders"), rowWithoutEndTime));
    WorkspaceClient client = newWorkspaceClient();
    assertTaskWritesExpectedCsv(
        "query_history_system_table.csv",
        DatabricksLogsHandle.forSystemTable(
            new DatabricksClientSqlRunner(client, "wh-1", Disposition.INLINE),
            client,
            WORKSPACE,
            ImmutableList.of(WORKSPACE)));
  }

  @Test
  public void probeStatementText_tellsARangeWithoutQueriesFromQueriesWithoutText()
      throws Exception {
    ImmutableList<String> workspaceIds = ImmutableList.of(WORKSPACE);
    String sql = QueryHistoryTask.systemTableSql(INTERVAL, workspaceIds);
    String textProbe = sql + " AND statement_text <> '' LIMIT 1";
    String blankTextProbe = sql + " AND (statement_text IS NULL OR statement_text = '') LIMIT 1";
    DatabricksClientSqlRunner sqlRunner =
        new DatabricksClientSqlRunner(newWorkspaceClient(), "wh-1", Disposition.INLINE);

    stubStatement(textProbe, ImmutableList.of(systemTableRow("stmt-1", "SELECT 1")));
    assertEquals(
        "SELECT 1", QueryHistoryTask.probeStatementText(sqlRunner, INTERVAL, workspaceIds));

    // No query has a statement text, so the second probe looks for a query without one.
    stubStatement(textProbe, ImmutableList.of());
    stubStatement(blankTextProbe, ImmutableList.of(systemTableRow("stmt-1", "")));
    assertEquals("", QueryHistoryTask.probeStatementText(sqlRunner, INTERVAL, workspaceIds));

    stubStatement(blankTextProbe, ImmutableList.of());
    assertNull(QueryHistoryTask.probeStatementText(sqlRunner, INTERVAL, workspaceIds));
  }

  @Test
  public void queryHistoryApi_writesExpectedCsvFromRecordedFixtures() throws Exception {
    JsonNode root =
        MAPPER.readTree(
            Resources.toString(
                Resources.getResource("connector/databricks/logs-responses.json"), UTF_8));
    for (Map.Entry<String, JsonNode> entry : root.properties()) {
      server.stubFor(
          get(urlEqualTo(entry.getKey())).willReturn(okJson(entry.getValue().toString())));
    }

    // The fixture URLs pin the requests. The API returns the queries that started at end_time_ms
    // too, so endExclusive - 1 is sent.
    assertTaskWritesExpectedCsv("query_history_api.csv", apiHandle(newWorkspaceClient()));
  }

  @Test
  public void queryHistoryApi_keepsTheValuesThatTheSdkModelsDrop() throws Exception {
    // VACUUM isn't a QueryStatementType of the SDK, and the SDK QueryMetrics has no
    // write_remote_rows.
    server.stubFor(
        get(urlPathEqualTo("/api/2.0/sql/history/queries"))
            .willReturn(
                okJson(
                    "{\"res\":[{\"query_id\":\"q-1\",\"statement_type\":\"VACUUM\","
                        + "\"status\":\"FINISHED\",\"metrics\":{\"write_remote_rows\":7}}],"
                        + "\"has_next_page\":false}")));

    CSVRecord record = runTaskForOnlyRecord(apiHandle(newWorkspaceClient()));

    assertEquals("VACUUM", record.get(Header.StatementType));
    assertEquals("7", record.get(Header.WrittenRows));
  }

  @Test
  public void queryHistoryApi_skipsTheQueriesThatHaventEnded() throws Exception {
    // The request has no status filter, so the API also returns the queries that are still queued
    // or running, which system.query.history doesn't have.
    server.stubFor(
        get(urlPathEqualTo("/api/2.0/sql/history/queries"))
            .willReturn(
                okJson(
                    "{\"res\":[{\"query_id\":\"q-1\",\"status\":\"QUEUED\"},"
                        + "{\"query_id\":\"q-2\",\"status\":\"RUNNING\"},"
                        + "{\"query_id\":\"q-3\",\"status\":\"CANCELED\"}],"
                        + "\"has_next_page\":false}")));

    CSVRecord record = runTaskForOnlyRecord(apiHandle(newWorkspaceClient()));

    assertEquals("q-3", record.get(Header.StatementId));
    assertEquals("CANCELED", record.get(Header.ExecutionStatus));
  }

  @Test
  public void queryHistoryApi_failsWhenAPageTokenRepeats() {
    // Otherwise, a page that returns its own token would be read forever.
    server.stubFor(
        get(urlPathEqualTo("/api/2.0/sql/history/queries"))
            .willReturn(
                okJson("{\"res\":[],\"has_next_page\":true,\"next_page_token\":\"same\"}")));

    IOException failure =
        assertThrows(IOException.class, () -> runTask(apiHandle(newWorkspaceClient())));

    assertTrue(failure.getMessage().contains("'same'"));
  }

  @Test
  public void queryHistoryApi_writesTheAccountIdOfTheClientConfiguration() throws Exception {
    // The API has no account ID, which the SDK reads from the host metadata of the workspace. This
    // server has no host metadata, so the configuration has the account ID that the test sets.
    server.stubFor(
        get(urlPathEqualTo("/api/2.0/sql/history/queries"))
            .willReturn(
                okJson(
                    "{\"res\":[{\"query_id\":\"q-1\",\"status\":\"FINISHED\"}],"
                        + "\"has_next_page\":false}")));
    WorkspaceClient client = new WorkspaceClient(newConfig().setAccountId(ACCOUNT));

    assertEquals(ACCOUNT, runTaskForOnlyRecord(apiHandle(client)).get(Header.AccountId));
    assertEquals("", runTaskForOnlyRecord(apiHandle(newWorkspaceClient())).get(Header.AccountId));
  }

  @Test
  public void systemTableSql_selectsTheHeaderColumnsOfTheIntervalAndWorkspaces() {
    assertEquals(
        "SELECT account_id, workspace_id, statement_id, session_id, executed_by, statement_text,"
            + " statement_type, execution_status, error_message, compute.warehouse_id,"
            + " compute.type, unix_millis(start_time), unix_millis(end_time), total_duration_ms,"
            + " compilation_duration_ms, execution_duration_ms, waiting_at_capacity_duration_ms,"
            + " read_bytes, read_rows, produced_rows, written_bytes, written_rows,"
            + " spilled_local_bytes, client_application, waiting_for_compute_duration_ms,"
            + " CAST(from_result_cache AS STRING) FROM system.query.history"
            + " WHERE start_time >= timestamp_millis("
            + START_TIME_MILLIS
            + ") AND start_time < timestamp_millis("
            + INTERVAL.getEndExclusive().toInstant().toEpochMilli()
            + ") AND COALESCE(try_element_at(query_tags, 'source'), '') <> 'dwh-migration-dumper'"
            + " AND workspace_id IN ('111', '222')",
        QueryHistoryTask.systemTableSql(INTERVAL, ImmutableList.of("111", "222")));
  }

  private static void assertTaskWritesExpectedCsv(
      String expectedCsvName, DatabricksLogsHandle handle) throws Exception {
    String expectedCsv =
        Resources.toString(
            Resources.getResource("connector/databricks/expected/" + expectedCsvName), UTF_8);
    assertEquals(expectedCsvName, expectedCsv.replace("\r\n", "\n"), runTask(handle));
  }

  /** Runs the task of the interval with {@code handle}, closes it, and returns the CSV. */
  private static String runTask(DatabricksLogsHandle handle) throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    try (DatabricksLogsHandle toClose = handle) {
      new QueryHistoryTask(getEntryFileNameWithTimestamp(ZIP_ENTRY_PREFIX, INTERVAL), INTERVAL)
          .doRun(null, sink, toClose);
    }

    return sink.openStream().toString().replace("\r\n", "\n");
  }

  /** Runs the task of the interval with {@code handle}, closes it, and returns the only record. */
  private static CSVRecord runTaskForOnlyRecord(DatabricksLogsHandle handle) throws Exception {
    try (CSVParser parser = CSVParser.parse(runTask(handle), FORMAT.withFirstRecordAsHeader())) {
      return Iterables.getOnlyElement(parser.getRecords());
    }
  }

  /** Returns the handle of a run that reads the query history API with {@code client}. */
  private static DatabricksLogsHandle apiHandle(WorkspaceClient client) {
    return DatabricksLogsHandle.forApi(client, WORKSPACE);
  }

  /** Stubs the Statement Execution API to return {@code rows} for the SQL {@code statement}. */
  private static void stubStatement(String statement, List<List<String>> rows) throws IOException {
    server.stubFor(
        post(urlPathEqualTo("/api/2.0/sql/statements"))
            .withRequestBody(matchingJsonPath("$.statement", equalTo(statement)))
            .willReturn(
                okJson(
                    "{\"status\":{\"state\":\"SUCCEEDED\"},\"result\":{\"data_array\":"
                        + MAPPER.writeValueAsString(rows)
                        + "}}")));
  }

  private static WorkspaceClient newWorkspaceClient() {
    return new WorkspaceClient(newConfig());
  }

  private static DatabricksConfig newConfig() {
    return new DatabricksConfig()
        .setHost(server.baseUrl())
        .setToken("test-token")
        .setAuthType("pat");
  }

  // The cells of a system.query.history row, in the order of Header, of a query that starts 1 s
  // into the interval, runs for 2.5 s without waiting for compute, and isn't read from the result
  // cache.
  private static List<String> systemTableRow(String statementId, String statementText) {
    long startMillis = START_TIME_MILLIS + 1_000L;
    return Splitter.on(',')
        .splitToList(
            String.format(
                "%s,%s,%s,session-1,user@example.com,%s,SELECT,FINISHED,,wh-1,WAREHOUSE,"
                    + "%d,%d,2500,100,2300,0,1024,10,1,0,0,0,Databricks SQL Editor,0,false",
                ACCOUNT, WORKSPACE, statementId, statementText, startMillis, startMillis + 2_500L));
  }
}
