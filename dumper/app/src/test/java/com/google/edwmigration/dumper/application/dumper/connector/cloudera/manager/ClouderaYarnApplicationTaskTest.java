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
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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

  private final ClouderaYarnApplicationsTask task = new ClouderaYarnApplicationsTask(30);
  private ClouderaManagerHandle handle;
  private static WireMockServer server;
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
  public void doRun_initiatedClusters_success() throws Exception {
    initClusters(ClouderaClusterDTO.create("cluster-id", "test-cluster"));

    stubServiceTypesAPI("test-cluster", "{}");
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("filter", matching("applicationType=MAPREDUCE"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "mapreduce");
    queryParams.put("filter", matching("applicationType=SPARK"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "spark");

    task.doRun(context, sink, handle);

    verify(1, getRequestedFor(urlEqualTo("/api/vTest/clusters/test-cluster/serviceTypes")));
    verify(
        2,
        getRequestedFor(
            urlEqualTo("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications")));
  }

  @Test
  public void doRun_noInitiatedClusters_throwsException() throws Exception {}

  @Test
  public void doRun_serviceTypesInCluster_requestsPredefinedTypes() throws Exception {}

  @Test
  public void doRun_failedServiceTypesAPI_throwsException() throws Exception {}

  @Test
  public void doRun_failedYARNApplicationsAPI_throwsException() throws Exception {}

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubServiceTypesAPI(String clusterName, String responseContent) {
    server.stubFor(
        get(String.format("/api/vTest/clusters/%s/serviceTypes", clusterName))
            .willReturn(okJson(responseContent).withStatus(HttpStatus.SC_OK)));
  }

  private void stubYARNApplicationsAPI(
      String clusterName, Map<String, StringValuePattern> queryParams, String responseContent) {
    server.stubFor(
        get(String.format("/api/vTest/clusters/%s/services/yarn/yarnApplications", clusterName))
            .withQueryParams(queryParams)
            .willReturn(okJson(responseContent).withStatus(HttpStatus.SC_OK)));
  }
}
