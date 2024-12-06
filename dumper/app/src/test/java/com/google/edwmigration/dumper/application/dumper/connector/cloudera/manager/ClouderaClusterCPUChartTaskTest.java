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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaClusterCPUChartTask.TimeSeriesAggregation;
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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
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
  private URI uri;

  @Before
  public void setUp() throws Exception {
    servicesJson = readFileAsString("/cloudera/manager/cluster-cpu-status.json");
    uri = URI.create("http://localhost/api");
    handle = new ClouderaManagerHandle(uri, httpClient);

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void noClusterId_skipWrites() throws Exception {
    // GIVEN: There is no cluster with a valid cluster ID
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create(null, "single cluster")));

    // WHEN
    task.doRun(context, sink, handle);

    // THEN: Task for such clusters should be skipped
    verify(httpClient, never()).execute(any());
    verifyNoWrites();
  }

  @Test
  public void validCluster_doWrites() throws Exception {
    // GIVEN: Valid two valid clusters
    handle.initClusters(
        ImmutableList.of(
            ClouderaClusterDTO.create("id1", "first-cluster"),
            ClouderaClusterDTO.create("id2", "second-cluster")));
    mockHttpRequest(servicesJson);

    // WHEN
    task.doRun(context, sink, handle);

    // THEN: the output should be dumped into the jsonl format for both clusters
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
    assertEquals(ImmutableSet.of(tojsonl(servicesJson), tojsonl(servicesJson)), fileLines);
  }

  private void mockHttpRequest(String mockedContent) throws IOException {
    CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
    HttpEntity httpEntity = mock(HttpEntity.class);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent())
        .thenReturn(
            new ByteArrayInputStream(mockedContent.getBytes()),
            new ByteArrayInputStream(mockedContent.getBytes()));
    when(httpClient.execute(argThat(Objects::nonNull))).thenReturn(httpResponse);
  }

  @Test
  public void clustersWereNotInitialized_throwsException() throws Exception {
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
  public void chartWithEmptyDateRange_throwsException() throws Exception {
    // GIVEN: There is a valid cluster
    handle.initClusters(ImmutableList.of(ClouderaClusterDTO.create("id1", "first-cluster")));

    // WHEN: CPU usage task with empty date range is initiated
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ClouderaClusterCPUChartTask(0, TimeSeriesAggregation.HOURLY));

    // THEN: A relevant exception has been raised
    assertEquals(
        "The chart has to include at least one day. Received 0 days.", exception.getMessage());
    verifyNoWrites();
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
}
