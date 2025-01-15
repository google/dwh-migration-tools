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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyChar;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaClusterCPUChartTaskTest {
  private final ClouderaClusterCPUChartTask task =
      new ClouderaClusterCPUChartTask(1, TimeSeriesAggregation.HOURLY);
  private ClouderaManagerHandle handle;
  private String servicesJson;

  @Mock private TaskRunContext context;
  @Mock private ByteSink sink;
  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    servicesJson = readFileAsString("/cloudera/manager/cluster-cpu-status.json");
    URI uri = URI.create("http://localhost/api");
    handle = new ClouderaManagerHandle(uri, httpClient);

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
    verify(httpClient, never()).execute(any());
    verifyNoWrites();
  }

  @Test
  public void doRun_missedAggregationParameter_throwsException() throws IOException {
    // WHEN: CPU usage task is initiated with no aggregation parameter
    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> new ClouderaClusterCPUChartTask(5, null));

    // THEN: There is a relevant exception has been raised
    assertEquals("TimeSeriesAggregation has not to be a null.", exception.getMessage());
    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturnsValidJson_writeJsonLines() throws Exception {
    // GIVEN: Two valid clusters
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id2", "second-cluster"));
    String firstClusterServicesJson = servicesJson;
    String secondClusterServicesJson = "{\"key\":" + servicesJson + "}";
    mockHttpRequestToFetchClusterCPUChart("id1", firstClusterServicesJson);
    mockHttpRequestToFetchClusterCPUChart("id2", secondClusterServicesJson);

    // WHEN:
    task.doRun(context, sink, handle);

    // THEN: the output should be dumped into the jsonl format for both clusters
    Set<String> fileLines = getWrittenJsonLines();
    assertEquals(
        ImmutableSet.of(tojsonl(firstClusterServicesJson), tojsonl(secondClusterServicesJson)),
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
    verifyNoWrites();
  }

  @Test
  public void initTask_requestChartWithEmptyDateRange_throwsException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));

    // WHEN: CPU usage task with empty date range is initiated
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ClouderaClusterCPUChartTask(0, TimeSeriesAggregation.HOURLY));

    // THEN: A relevant exception has been raised
    assertEquals(
        "The chart has to include at least one day. Received 0 days.", exception.getMessage());
  }

  @Test
  public void doRun_clouderaReturns4xx_throwsCriticalException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = servicesJson;
    mockHttpRequestToFetchClusterCPUChart(
        "id1", firstClusterServicesJson, HttpStatus.SC_BAD_REQUEST);

    // WHEN: Cloudera returns 4xx http status code
    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    // THEN: There is a relevant exception has been raised
    assertTrue(exception.getMessage().contains("Cloudera Error: "));
    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturns5xx_throwsCriticalException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = servicesJson;
    mockHttpRequestToFetchClusterCPUChart(
        "id1", firstClusterServicesJson, HttpStatus.SC_INTERNAL_SERVER_ERROR);

    // WHEN: Cloudera returns 4xx http status code
    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    // THEN: There is a relevant exception has been raised
    assertTrue(exception.getMessage().contains("Cloudera Error: "));
    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturnsInvalidJson_throwsCriticalException() throws Exception {
    // GIVEN: There is a valid cluster
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String firstClusterServicesJson = "{\"key\": []]";
    mockHttpRequestToFetchClusterCPUChart("id1", firstClusterServicesJson);

    // WHEN: Cloudera returns 4xx http status code
    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    // THEN: There is a relevant exception has been raised
    assertTrue(exception.getMessage().contains("Cloudera Error: "));
    verifyNoWrites();
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void mockHttpRequestToFetchClusterCPUChart(String clusterName, String mockedContent)
      throws IOException {
    mockHttpRequestToFetchClusterCPUChart(clusterName, mockedContent, HttpStatus.SC_OK);
  }

  private void mockHttpRequestToFetchClusterCPUChart(
      String clusterName, String mockedContent, int statusCode) throws IOException {
    CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
    HttpEntity httpEntity = mock(HttpEntity.class);
    mockStatusLine(httpResponse, statusCode);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(mockedContent.getBytes()));
    when(httpClient.execute(
            argThat(
                (HttpGet get) ->
                    get != null
                        && get.getURI()
                            .getQuery()
                            .contains(String.format("entityName = \"%s\"", clusterName)))))
        .thenReturn(httpResponse);
  }

  private void mockStatusLine(CloseableHttpResponse responseHost, int statusCode) {
    StatusLine statusLine = mock();
    when(responseHost.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(statusCode);
  }

  private void verifyNoWrites() throws IOException {
    verify(writer, never()).write(anyChar());
    verify(writer, never()).write(anyString());
    verify(writer, never()).write(anyString(), anyInt(), anyInt());
    verify(writer, never()).write(any(char[].class));
    verify(writer, never()).write(any(char[].class), anyInt(), anyInt());
  }

  private Set<String> getWrittenJsonLines() throws IOException {
    // https://jsonlines.org/
    Set<String> fileLines = new HashSet<>();
    verify(writer, times(2))
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

  private String readFileAsString(String fileName) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(fileName).toURI())));
  }

  private String tojsonl(String json) throws Exception {
    return new ObjectMapper().readTree(json).toString();
  }
}
