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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.URI;
import java.util.List;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
public class SparkHistoryDiscoveryServiceTest {

  private static WireMockServer server;
  private SparkHistoryDiscoveryService service;
  private CloseableHttpClient cmClient;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Mock private CloseableHttpClient knoxClient;

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
  public void setUp() {
    server.resetAll();
    cmClient = HttpClients.createDefault();
    URI apiUri = URI.create(server.baseUrl() + "/api/v41/");
    service = new SparkHistoryDiscoveryService(objectMapper, cmClient, apiUri);
  }

  @Test
  public void resolveUrl_spark3_success() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe for Spark 3
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    when(knoxClient.execute(any(HttpGet.class)))
        .thenAnswer(
            invocation -> {
              HttpGet get = invocation.getArgument(0);
              if (get.getURI().toString().contains("spark3history")) {
                return mockResponse;
              }
              CloseableHttpResponse failResponse = mock(CloseableHttpResponse.class);
              StatusLine failStatus = mock(StatusLine.class);
              when(failStatus.getStatusCode()).thenReturn(404);
              when(failResponse.getStatusLine()).thenReturn(failStatus);
              return failResponse;
            });

    // Act
    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    // Assert
    assertEquals(1, result.size());
    assertEquals("https://knox-host/gateway/cdp-proxy-api/spark3history/api/v1", result.get(0));
  }

  @Test
  public void resolveUrl_spark2_success() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe for Spark 2 (Spark 3 fails)
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    when(knoxClient.execute(any(HttpGet.class)))
        .thenAnswer(
            invocation -> {
              HttpGet get = invocation.getArgument(0);
              if (get.getURI().toString().contains("sparkhistory")) {
                return mockResponse;
              }
              CloseableHttpResponse failResponse = mock(CloseableHttpResponse.class);
              StatusLine failStatus = mock(StatusLine.class);
              when(failStatus.getStatusCode()).thenReturn(404);
              when(failResponse.getStatusLine()).thenReturn(failStatus);
              return failResponse;
            });

    // Act
    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    // Assert
    assertEquals(1, result.size());
    assertEquals("https://knox-host/gateway/cdp-proxy-api/sparkhistory/api/v1", result.get(0));
  }

  @Test
  public void resolveUrl_multipleReachable_success() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe for both Spark 2 and Spark 3
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    when(knoxClient.execute(any(HttpGet.class))).thenReturn(mockResponse);

    // Act
    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    // Assert
    assertEquals(2, result.size());
    assertTrue(result.contains("https://knox-host/gateway/cdp-proxy-api/spark3history/api/v1"));
    assertTrue(result.contains("https://knox-host/gateway/cdp-proxy-api/sparkhistory/api/v1"));
  }

  @Test
  public void resolveUrl_customCandidate_success() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe for custom path
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    when(knoxClient.execute(any(HttpGet.class)))
        .thenAnswer(
            invocation -> {
              HttpGet get = invocation.getArgument(0);
              if (get.getURI().toString().contains("custom-spark")) {
                return mockResponse;
              }
              CloseableHttpResponse failResponse = mock(CloseableHttpResponse.class);
              StatusLine failStatus = mock(StatusLine.class);
              when(failStatus.getStatusCode()).thenReturn(404);
              when(failResponse.getStatusLine()).thenReturn(failStatus);
              return failResponse;
            });

    // Act
    List<String> result =
        service.resolveUrl(
            "my-cluster", knoxClient, com.google.common.collect.ImmutableList.of("custom-spark"));

    // Assert
    assertEquals(1, result.size());
    assertEquals("https://knox-host/gateway/cdp-proxy-api/custom-spark/api/v1", result.get(0));
  }

  @Test
  public void resolveUrl_neitherSparkVersionReachable_returnsEmpty() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe always returning 404
    CloseableHttpResponse failResponse = mock(CloseableHttpResponse.class);
    StatusLine failStatus = mock(StatusLine.class);
    when(failStatus.getStatusCode()).thenReturn(404);
    when(failResponse.getStatusLine()).thenReturn(failStatus);
    when(knoxClient.execute(any(HttpGet.class))).thenReturn(failResponse);

    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    assertTrue(result.isEmpty());
  }

  @Test
  public void resolveUrl_missingHostRef_returnsEmpty() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list with missing hostRef
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // Act
    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    // Assert
    assertTrue(result.isEmpty());
  }

  @Test
  public void resolveUrl_noKnoxService_returnsEmpty() throws Exception {
    // Mock empty services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services").willReturn(okJson("{\"items\": []}")));

    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    assertTrue(result.isEmpty());
  }

  @Test
  public void resolveUrl_cmApiError_returnsEmpty() throws Exception {
    // Mock CM API returning 500
    server.stubFor(get("/api/v41/clusters/my-cluster/services").willReturn(serverError()));

    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    assertTrue(result.isEmpty());
  }

  @Test
  public void resolveUrl_knoxNotReachable_returnsEmpty() throws Exception {
    // 1. Mock services list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services")
            .willReturn(okJson("{\"items\": [{\"name\": \"knox-1\", \"type\": \"KNOX\"}]}")));

    // 2. Mock roles list
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roles")
            .willReturn(
                okJson(
                    "{\"items\": [{\"hostRef\": {\"hostname\": \"knox-host\"}, \"roleConfigGroupRef\": {\"roleConfigGroupName\": \"knox-BASE\"}}]}")));

    // 3. Mock config
    server.stubFor(
        get("/api/v41/clusters/my-cluster/services/knox-1/roleConfigGroups/knox-BASE/config")
            .willReturn(
                okJson(
                    "{\"items\": [{\"name\": \"gateway_path\", \"value\": \"gateway\"}, {\"name\": \"gateway_default_api_topology_name\", \"value\": \"cdp-proxy-api\"}]}")));

    // 4. Mock Knox probe always failing
    when(knoxClient.execute(any(HttpGet.class)))
        .thenThrow(new java.io.IOException("Connection refused"));

    List<String> result =
        service.resolveUrl("my-cluster", knoxClient, com.google.common.collect.ImmutableList.of());

    assertTrue(result.isEmpty());
  }
}
