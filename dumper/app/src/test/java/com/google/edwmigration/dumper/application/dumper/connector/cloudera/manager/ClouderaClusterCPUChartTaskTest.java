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
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.MockUtils.verifyNoWrites;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.TestUtils.readFileAsString;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.TestUtils.toJsonl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
public class ClouderaClusterCPUChartTaskTest {
  private final ClouderaClusterCPUChartTask task =
      new ClouderaClusterCPUChartTask(
          timeTravelDaysAgo(30),
          timeTravelDaysAgo(0),
          TimeSeriesAggregation.HOURLY,
          TaskCategory.REQUIRED);
  private ClouderaManagerHandle handle;
  private String servicesJson;
  private static WireMockServer server;

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
    servicesJson = readFileAsString("/cloudera/manager/cluster-cpu-status.json");
    URI uri = URI.create(server.baseUrl() + "/api/vTest");
    handle =
        new ClouderaManagerHandle(uri, HttpClients.createDefault(), HttpClients.createDefault());

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void doRun_initiatedClusterWithoutId_skipWrites() throws Exception {
    // GIVEN: There is no cluster with a valid cluster ID
    initClusters(ClouderaClusterDTO.create(null, "single cluster"));

    // WHEN
    task.doRun(context, sink, handle);

    // THEN: Task for such clusters should be skipped
    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaReturnsValidJson_writeJsonLines() throws Exception {
    // GIVEN: Two valid clusters
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id2", "second-cluster"));
    String firstClusterServicesJson = servicesJson;
    String secondClusterServicesJson = "{\"key\":" + servicesJson + "}";
    stubHttpRequestToFetchClusterCPUChart("id1", firstClusterServicesJson);
    stubHttpRequestToFetchClusterCPUChart("id2", secondClusterServicesJson);

    // WHEN:
    task.doRun(context, sink, handle);

    // THEN: the output should be dumped into the jsonl format for both clusters
    Set<String> fileLines = new HashSet<>(MockUtils.getWrittenJsonLines(writer, 2));
    assertEquals(
        ImmutableSet.of(toJsonl(firstClusterServicesJson), toJsonl(secondClusterServicesJson)),
        fileLines);
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsException() throws Exception {
    // GIVEN: There is no valid cluster
    assertNull(handle.getClusters());

    // WHEN:
    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    // THEN: There is a relevant exception has been raised
    assertEquals(
        "Cloudera clusters must be initialized before CPU charts dumping.", exception.getMessage());
    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaReturns4xx_throwsException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = servicesJson;
    stubHttpRequestToFetchClusterCPUChart(
        "id1", firstClusterServicesJson, HttpStatus.SC_BAD_REQUEST);

    // WHEN: Cloudera returns 4xx http status code
    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaReturns5xx_throwsException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = servicesJson;
    stubHttpRequestToFetchClusterCPUChart(
        "id1", firstClusterServicesJson, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    // WHEN: Cloudera returns 4xx http status code
    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    // THEN: There is a relevant exception has been raised
    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaReturnsInvalidJson_throwsException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = "{\"key\": []]";
    stubHttpRequestToFetchClusterCPUChart("id1", firstClusterServicesJson);

    // WHEN: Cloudera returns 4xx http status code
    assertThrows(JsonParseException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites(writer);
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubHttpRequestToFetchClusterCPUChart(String clusterName, String mockedContent)
      throws IOException {
    stubHttpRequestToFetchClusterCPUChart(clusterName, mockedContent, HttpStatus.SC_OK);
  }

  private void stubHttpRequestToFetchClusterCPUChart(
      String clusterName, String mockedContent, int statusCode) throws IOException {
    server.stubFor(
        get(urlMatching(String.format("/api/vTest/timeseries.*%s.*", clusterName)))
            .willReturn(okJson(mockedContent).withStatus(statusCode)));
  }

  private ZonedDateTime timeTravelDaysAgo(int days) {
    ZonedDateTime today = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));
    return today.minusDays(days);
  }
}
