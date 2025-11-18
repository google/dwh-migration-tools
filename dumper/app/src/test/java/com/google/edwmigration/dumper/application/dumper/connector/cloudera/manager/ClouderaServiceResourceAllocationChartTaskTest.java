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
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
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
public class ClouderaServiceResourceAllocationChartTaskTest {

  private static WireMockServer server;
  private final ClouderaServiceResourceAllocationChartTask task =
      new ClouderaServiceResourceAllocationChartTask(
          timeTravelDaysAgo(30),
          timeTravelDaysAgo(0),
          TimeSeriesAggregation.HOURLY,
          TaskCategory.OPTIONAL);
  private ClouderaManagerHandle handle;

  @Mock private TaskRunContext context;
  @Mock private ByteSink sink;
  @Mock private Writer writer;
  @Mock private CharSink charSink;

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
    server.resetAll();
    URI uri = URI.create(server.baseUrl() + "/api/vTest");
    handle = new ClouderaManagerHandle(uri, HttpClients.createDefault());

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void doRun_initiatedHostWithoutId_skipWrites() throws Exception {
    initHosts(ClouderaHostDTO.create(null, "host1"));

    task.doRun(context, sink, handle);

    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturnsValidJson_writeJsonLines() throws Exception {
    String firstHostId = "becfcf668a1861ab926151f1b11a726d";
    String secondHostId = "abcdef668a1861ab926151f1b11a726d";
    initHosts(
        ClouderaHostDTO.create(firstHostId, "host1"),
        ClouderaHostDTO.create(secondHostId, "host2"));
    String firstHostServicesResourceAllocationJson =
        readFileAsString("/cloudera/manager/host-services-resource-allocation-1.json");
    String secondHostServicesResourceAllocationJson =
        readFileAsString("/cloudera/manager/host-services-resource-allocation-2.json");
    stubHttpRequestToFetchHostServicesResourceAllocationChart(
        firstHostId, firstHostServicesResourceAllocationJson);
    stubHttpRequestToFetchHostServicesResourceAllocationChart(
        secondHostId, secondHostServicesResourceAllocationJson);

    task.doRun(context, sink, handle);

    Set<String> fileLines = new HashSet<>(MockUtils.getWrittenJsonLines(writer, 2));
    assertEquals(
        ImmutableSet.of(
            tojsonl(firstHostServicesResourceAllocationJson),
            tojsonl(secondHostServicesResourceAllocationJson)),
        fileLines);
  }

  @Test
  public void doRun_hostsWereNotInitialized_throwsException() throws Exception {
    assertNull(handle.getHosts());

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera hosts must be initialized before service resource allocation charts dumping.",
        exception.getMessage());
    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturns4xx_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "host1"));
    String hostServicesResourceAllocationJson =
        readFileAsString("/cloudera/manager/host-services-resource-allocation-1.json");
    stubHttpRequestToFetchHostServicesResourceAllocationChart(
        "id1", hostServicesResourceAllocationJson, HttpStatus.SC_BAD_REQUEST);

    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturns5xx_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "host1"));
    String hostServicesResourceAllocationJson =
        readFileAsString("/cloudera/manager/host-services-resource-allocation-1.json");
    stubHttpRequestToFetchHostServicesResourceAllocationChart(
        "id1", hostServicesResourceAllocationJson, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturnsInvalidJson_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "host1"));
    String invalidJson = "{\"key\": []]";
    stubHttpRequestToFetchHostServicesResourceAllocationChart("id1", invalidJson);

    assertThrows(JsonParseException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites();
  }

  private void initHosts(ClouderaHostDTO... hosts) {
    handle.initHosts(Arrays.asList(hosts));
  }

  private void stubHttpRequestToFetchHostServicesResourceAllocationChart(
      String hostId, String mockedContent) throws IOException {
    stubHttpRequestToFetchHostServicesResourceAllocationChart(
        hostId, mockedContent, HttpStatus.SC_OK);
  }

  private void stubHttpRequestToFetchHostServicesResourceAllocationChart(
      String hostId, String mockedContent, int statusCode) throws IOException {
    server.stubFor(
        get(urlMatching(String.format("/api/vTest/timeseries.*%s.*", hostId)))
            .willReturn(okJson(mockedContent).withStatus(statusCode)));
  }

  private void verifyNoWrites() throws IOException {
    verify(writer, never()).write(anyChar());
    verify(writer, never()).write(anyString());
    verify(writer, never()).write(anyString(), anyInt(), anyInt());
    verify(writer, never()).write(any(char[].class));
    verify(writer, never()).write(any(char[].class), anyInt(), anyInt());
  }

  private String readFileAsString(String fileName) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(fileName).toURI())));
  }

  private String tojsonl(String json) throws Exception {
    return new ObjectMapper().readTree(json).toString();
  }

  private ZonedDateTime timeTravelDaysAgo(int days) {
    ZonedDateTime today = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));
    return today.minusDays(days);
  }
}
