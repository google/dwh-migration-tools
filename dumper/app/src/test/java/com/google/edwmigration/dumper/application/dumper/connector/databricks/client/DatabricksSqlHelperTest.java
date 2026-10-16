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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksSqlHelperTest {

  private static final String CATALOG = "hive_metastore";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final ObjectWriter WRITER = SerDeUtils.createMapper().writer();

  private WireMockServer wireMock;

  @Before
  public void setUp() {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort());
    wireMock.start();
  }

  @After
  public void tearDown() {
    if (wireMock != null) {
      wireMock.stop();
    }
  }

  @Test
  public void sqlHelper_pollsStatementAndReadsEveryChunkWithRetry() throws Exception {
    // The statement is PENDING, then SUCCEEDED with 2 chunks, and the download of the first chunk
    // fails once with HTTP 503.
    StatementResponse succeeded =
        statement(StatementState.SUCCEEDED)
            .setManifest(new ResultManifest().setTotalChunkCount(2L))
            .setResult(chunk("/chunk/0"));
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .willReturn(okModel(statement(StatementState.PENDING))));
    wireMock.stubFor(
        get(urlPathEqualTo(STATEMENTS_PATH + "/stmt-1")).willReturn(okModel(succeeded)));
    wireMock.stubFor(
        get(urlPathEqualTo(STATEMENTS_PATH + "/stmt-1/result/chunks/1"))
            .willReturn(okModel(chunk("/chunk/1"))));
    wireMock.stubFor(
        get(urlPathEqualTo("/chunk/0"))
            .inScenario("retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(503))
            .willSetStateTo("failed once"));
    wireMock.stubFor(
        get(urlPathEqualTo("/chunk/0"))
            .inScenario("retry")
            .whenScenarioStateIs("failed once")
            .willReturn(okJson("[[\"r0\"], [\"r1\"]]")));
    wireMock.stubFor(get(urlPathEqualTo("/chunk/1")).willReturn(okJson("[[\"r2\"]]")));

    StatementResult result =
        new DatabricksSqlHelper(newWorkspaceClient(), "wh-1", Duration.ZERO, Duration.ofMinutes(1))
            .executeSqlInCatalog(CATALOG, "SELECT 1");
    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    for (List<String> row = result.nextRow(); row != null; row = result.nextRow()) {
      rows.add(row);
    }

    assertEquals(
        ImmutableList.of(ImmutableList.of("r0"), ImmutableList.of("r1"), ImmutableList.of("r2")),
        rows.build());
  }

  @Test
  public void sqlHelper_cancelsStatementWhenInterrupted() {
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .willReturn(okModel(statement(StatementState.RUNNING))));
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH + "/stmt-1/cancel")).willReturn(okJson("{}")));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(newWorkspaceClient(), "wh-1", Duration.ZERO, Duration.ofMinutes(1));

    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedIOException.class, () -> helper.executeSqlInCatalog(CATALOG, "SELECT 1"));
    } finally {
      // Clears the interrupt flag for the next tests.
      Thread.interrupted();
    }

    wireMock.verify(1, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH + "/stmt-1/cancel")));
  }

  @Test
  public void sqlQuoting_quotesIdentifiersQualifiedNamesAndStringLiterals() {
    assertEquals("`odd``name`", SqlQuoting.quoteIdentifier("odd`name"));
    assertEquals("`s``1`.`t``2`", SqlQuoting.quoteQualifiedName("s`1", "t`2"));
    assertEquals("sales_2024", SqlQuoting.quoteIdentifierIfNeeded("sales_2024"));
    assertEquals("`2024`", SqlQuoting.quoteIdentifierIfNeeded("2024"));
    assertEquals("`my table`", SqlQuoting.quoteIdentifierIfNeeded("my table"));
    assertEquals("'it\\'s C:\\\\tmp'", SqlQuoting.quoteStringLiteral("it's C:\\tmp"));
  }

  private static StatementResponse statement(StatementState state) {
    return new StatementResponse()
        .setStatementId("stmt-1")
        .setStatus(new StatementStatus().setState(state));
  }

  /** Returns a result chunk with one external link to {@code path} on the WireMock server. */
  private ResultData chunk(String path) {
    return new ResultData()
        .setExternalLinks(
            ImmutableList.of(new ExternalLink().setExternalLink(wireMock.baseUrl() + path)));
  }

  private WorkspaceClient newWorkspaceClient() {
    return new WorkspaceClient(
        new DatabricksConfig()
            .setHost(wireMock.baseUrl())
            .setToken("test-token")
            .setAuthType("pat"));
  }

  private static ResponseDefinitionBuilder okModel(Object model) {
    try {
      return okJson(WRITER.writeValueAsString(model));
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Failed to write a test response.", e);
    }
  }
}
