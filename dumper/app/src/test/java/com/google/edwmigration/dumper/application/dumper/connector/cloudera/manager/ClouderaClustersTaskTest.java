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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.TestUtils.readFileAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiClusterListDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaClustersTaskTest {
  private static WireMockServer server;

  private final ClouderaClustersTask task = new ClouderaClustersTask();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private ClouderaManagerHandle handle;
  @Mock private TaskRunContext context;
  @Mock private ConnectorArguments arguments;
  @Mock private ByteSink sink;

  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private CloseableHttpClient httpClient;

  private String apiClusterListJson;
  private String apiClusterJson;
  private String clusterStatusJson;
  private final int clusterStatusId = 1546336862;

  @BeforeClass
  public static void beforeClass() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    server.stop();
  }

  @Before
  public void setUp() throws Exception {
    server.resetAll(); // reset request/response stubs
    handle = new ClouderaManagerHandle(URI.create("http://localhost/api"), httpClient);

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
    when(context.getArguments()).thenReturn(arguments);

    apiClusterListJson = readFileAsString("/cloudera/manager/dto/ApiClusterList.json");
    apiClusterJson = readFileAsString("/cloudera/manager/dto/ApiCluster.json");
    clusterStatusJson = readFileAsString("/cloudera/manager/cluster-status.json");
  }

  @Test
  public void doRun_clusterNotProvided_fetchAllClusters() throws Exception {
    when(arguments.getCluster()).thenReturn(null);
    URI apiUrl = URI.create(server.baseUrl() + "/api/vTest/");
    handle = new ClouderaManagerHandle(apiUrl, HttpClients.createDefault());

    server.stubFor(
        get("/api/vTest/clusters?clusterType=ANY").willReturn(okJson(apiClusterListJson)));
    server.stubFor(
        get("/cmf/clusters/aaa/status.json").willReturn(okJson(clusterStatusJsonWithId("111"))));
    server.stubFor(
        get("/cmf/clusters/bbb/status.json")
            .willReturn(aResponse().withStatus(401).withBody(clusterStatusJsonWithId("222"))));

    task.doRun(context, sink, handle);

    server.verify(getRequestedFor(urlEqualTo("/api/vTest/clusters?clusterType=ANY")));
    server.verify(getRequestedFor(urlEqualTo("/cmf/clusters/aaa/status.json")));
    server.verify(getRequestedFor(urlEqualTo("/cmf/clusters/bbb/status.json")));
    assertTrue(server.findAllUnmatchedRequests().isEmpty());

    verify(writer)
        .write(
            (String)
                argThat(
                    content -> {
                      try {
                        ApiClusterListDTO listDto =
                            objectMapper.readValue((String) content, ApiClusterListDTO.class);
                        assertNotNull(listDto.getClusters());
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      return true;
                    }));

    ImmutableList<ClouderaClusterDTO> clusters = handle.getClusters();
    assertNotNull(clusters);
    assertEquals(
        ImmutableList.of(
            ClouderaClusterDTO.create("111", "aaa"), ClouderaClusterDTO.create(null, "bbb")),
        clusters);
    verify(writer).close();
  }

  @Test
  public void doRun_clusterProvided_fetchOnlyProvidedCluster() throws Exception {
    when(arguments.getCluster()).thenReturn("my-cluster");
    URI apiUrl = URI.create(server.baseUrl() + "/api/vTest/");
    handle = new ClouderaManagerHandle(apiUrl, HttpClients.createDefault());

    server.stubFor(get("/api/vTest/clusters/my-cluster").willReturn(okJson(apiClusterJson)));
    server.stubFor(
        get("/cmf/clusters/my-cluster/status.json")
            .willReturn(okJson(clusterStatusJsonWithId("123"))));

    task.doRun(context, sink, handle);

    server.verify(getRequestedFor(urlEqualTo("/api/vTest/clusters/my-cluster")));
    server.verify(getRequestedFor(urlEqualTo("/cmf/clusters/my-cluster/status.json")));
    assertTrue(server.findAllUnmatchedRequests().isEmpty());

    verify(writer)
        .write(
            (String)
                argThat(
                    content -> {
                      try {
                        ApiClusterListDTO listDto =
                            objectMapper.readValue((String) content, ApiClusterListDTO.class);
                        assertNotNull(listDto.getClusters());
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      return true;
                    }));

    ImmutableList<ClouderaClusterDTO> clusters = handle.getClusters();
    assertNotNull(clusters);
    assertEquals(ImmutableList.of(ClouderaClusterDTO.create("123", "my-cluster")), clusters);

    verify(writer).close();
  }

  private String clusterStatusJsonWithId(String clusterId) {
    return clusterStatusJson.replaceAll("" + clusterStatusId, clusterId);
  }
}
