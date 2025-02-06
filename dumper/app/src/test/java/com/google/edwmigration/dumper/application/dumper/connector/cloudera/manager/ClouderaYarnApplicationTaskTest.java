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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaYarnApplicationTaskTest {
  private static WireMockServer server;

  private final ClouderaYarnApplicationsTask task = new ClouderaYarnApplicationsTask(30);
  private ClouderaManagerHandle handle;

  @Mock private TaskRunContext context;
  @Mock private ByteSink sink;
  @Mock private Writer writer;
  @Mock private CharSink charSink;

  @BeforeClass
  public static void beforeClass() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void afterClass() {
    server.stop();
  }

  @Before
  public void setUp() throws Exception {
    server.resetAll();
    URI uri = URI.create(server.baseUrl() + "/api/vTest/");
    handle = new ClouderaManagerHandle(uri, HttpClients.createDefault());
    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void doRun_twoPaginatedPage_success() throws Exception {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("limit", matching("1000"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\": [{\"applicationId\":\"app1\"}]}");

    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\": []}");

    task.doRun(context, sink, handle);

    List<String> lines = new ArrayList<>(getWrittenJsonLines());
    assertEquals(lines.size(), 1);
    assertTrue(lines.get(0).contains("\"applicationId\":\"app1\""));
    server.verify(0, getRequestedFor(urlEqualTo("/api/vTest/clusters/test-cluster/serviceTypes")));
    server.verify(
        2,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
  }

  @Test
  public void doRun_noInitiatedClusters_throwsException() {
    assertNull(handle.getClusters());

    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Clusters must be initialized before fetching YARN applications.", exception.getMessage());
  }

  @Test
  public void doRun_failedYARNApplicationsAPI_throwsException() {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "Internal server error", HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera Error: YARN application API returned HTTP status 500.", exception.getMessage());
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubYARNApplicationsAPI(
      String clusterName, Map<String, StringValuePattern> queryParams, String responseContent) {
    stubYARNApplicationsAPI(clusterName, queryParams, responseContent, HttpStatus.SC_OK);
  }

  private void stubYARNApplicationsAPI(
      String clusterName,
      Map<String, StringValuePattern> queryParams,
      String responseContent,
      int statusCode) {
    server.stubFor(
        get(urlPathMatching(
                String.format(
                    "/api/vTest/clusters/%s/services/yarn/yarnApplications.*", clusterName)))
            .withQueryParams(queryParams)
            .willReturn(okJson(responseContent).withStatus(statusCode)));
  }

  private Set<String> getWrittenJsonLines() throws IOException {
    // https://jsonlines.org/
    Set<String> fileLines = new HashSet<>();
    verify(writer, times(1))
        .write(
            (String)
                argThat(
                    content -> {
                      String str = (String) content;
                      assertFalse(str.contains("\n"));
                      assertFalse(str.contains("\r"));
                      fileLines.add(str);
                      return true;
                    }));
    return fileLines;
  }
}
