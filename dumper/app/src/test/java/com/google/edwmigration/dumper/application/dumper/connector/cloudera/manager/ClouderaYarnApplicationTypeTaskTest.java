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
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaYarnApplicationTypeTaskTest {
  private static WireMockServer server;
  private final ClouderaYarnApplicationTypeTask task = new ClouderaYarnApplicationTypeTask(30);
  private ClouderaManagerHandle handle;

  @Mock private ByteSink sink;
  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private TaskRunContext context;
  @Mock private ConnectorArguments cliArgs;

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

    when(context.getArguments()).thenReturn(cliArgs);
    when(cliArgs.getPaginationPageSize()).thenReturn(1000);
    when(cliArgs.getYarnApplicationTypes()).thenReturn(new ArrayList<>());
  }

  @Test
  public void doRun_initializedClusters_writesMappingsInJsonLines() throws Exception {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));

    // Cloudera API which returns service types per cluster
    stubYARNApplicationTypesAPI("test-cluster", "{\"items\":[\"CLOUDERA_TYPE\"]}");

    // Stub API for predefined SPARK application type
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("filter", matching("applicationType=SPARK"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\":[{\"applicationId\":\"spark-app\"}]}");
    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    // Stub API for predefined MAPREDUCE application type
    queryParams.put("filter", matching("applicationType=MAPREDUCE"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\":[{\"applicationId\":\"mapreduce-app\"}]}");
    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    // Stub API for application types from Cloudera
    queryParams.put("filter", matching("applicationType=CLOUDERA_TYPE"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\":[{\"applicationId\":\"custom-app\"}]}");
    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    task.doRun(context, sink, handle);

    // Because of pagination application API will be called twice per application type
    server.verify(
        6,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
    server.verify(
        1, getRequestedFor(urlPathMatching("/api/vTest/clusters/test-cluster/serviceTypes.*")));
    List<String> fileJsonLines = MockUtils.getWrittenJsonLines(writer, 3);
    Assert.assertEquals(3, fileJsonLines.size());

    String combinedLine = fileJsonLines.get(0) + fileJsonLines.get(1) + fileJsonLines.get(2);
    Assert.assertTrue(combinedLine.contains("SPARK"));
    Assert.assertTrue(combinedLine.contains("MAPREDUCE"));
    Assert.assertTrue(combinedLine.contains("CLOUDERA_TYPE"));
  }

  @Test
  public void doRun_noClusters_throwsException() {
    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Clusters must be initialized before fetching YARN application types.",
        exception.getMessage());
  }

  @Test
  public void doRun_notPositiveDays_throwsException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new ClouderaYarnApplicationTypeTask(0));

    assertEquals("Amount of days must be a positive number. Got 0.", exception.getMessage());
  }

  @Test
  public void doRun_clouderaServerError_throwsException() {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));
    stubYARNApplicationTypesAPI(
        "test-cluster", "{\"items\":[\"CLOUDERA_TYPE\"]}", HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    assertEquals("Cloudera API returned bad http status: 500", exception.getMessage());
  }

  @Test
  public void doRun_applicationTypesFromCLI_writesMappingsInJsonLines() throws Exception {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));
    when(cliArgs.getYarnApplicationTypes()).thenReturn(Arrays.asList("CUSTOM-YARN-APP-TYPE"));
    stubYARNApplicationTypesAPI("test-cluster", "{\"items\":[]}");

    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("filter", matching("applicationType=SPARK"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    queryParams.put("filter", matching("applicationType=MAPREDUCE"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    queryParams.put("filter", matching("applicationType=CUSTOM-YARN-APP-TYPE"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\":[{\"applicationId\":\"custom-app\"}]}");
    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\":[]}");

    task.doRun(context, sink, handle);

    server.verify(
        4,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
    List<String> fileJsonLines = MockUtils.getWrittenJsonLines(writer, 1);
    Assert.assertEquals(1, fileJsonLines.size());
    Assert.assertTrue(fileJsonLines.get(0).contains("CUSTOM-YARN-APP-TYPE"));
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

  private void stubYARNApplicationTypesAPI(String clusterName, String responseContent) {
    stubYARNApplicationTypesAPI(clusterName, responseContent, HttpStatus.SC_OK);
  }

  private void stubYARNApplicationTypesAPI(
      String clusterName, String responseContent, int statusCode) {
    server.stubFor(
        get(urlPathMatching(String.format("/api/vTest/clusters/%s/serviceTypes.*", clusterName)))
            .willReturn(okJson(responseContent).withStatus(statusCode)));
  }
}
