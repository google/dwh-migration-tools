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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
public class ClouderaAPIHostsTaskTest {

  private final ClouderaAPIHostsTask task = new ClouderaAPIHostsTask();
  private ClouderaManagerHandle handle;

  @Mock private TaskRunContext context;
  @Mock private ByteSink sink;

  @Mock private Writer writer;
  @Mock private CharSink charSink;
  @Mock private CloseableHttpClient httpClient;

  @Before
  public void setUp() throws Exception {
    URI uri = URI.create("http://localhost/api");
    handle = new ClouderaManagerHandle(uri, httpClient);

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void doRun_clouderaReturnsValidJsonLines_writeJsonLines() throws Exception {
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id125", "second-cluster"));

    CloseableHttpResponse resp1 = mockClouderaClusterAPIResponse("first-cluster", "[]");
    CloseableHttpResponse resp2 = mockClouderaClusterAPIResponse("second-cluster", "[]\r\n");

    task.doRun(context, sink, handle);

    Set<URI> requestedUrls = getCalledURLs();
    assertEquals(
        ImmutableSet.of(
            URI.create("http://localhost/api/clusters/first-cluster/hosts"),
            URI.create("http://localhost/api/clusters/second-cluster/hosts")),
        requestedUrls);

    // write jsonl. https://jsonlines.org/
    Set<String> fileLines = getWrittenJsonLines();
    verify(writer, times(2)).write('\n');
    assertEquals(
        ImmutableSet.of(
            "{\"clusterName\":\"first-cluster\",\"items\":[]}",
            "{\"clusterName\":\"second-cluster\",\"items\":[]}"),
        fileLines);

    verify(resp1).close();
    verify(resp2).close();
    verify(writer).close();
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsCriticalException() throws Exception {
    assertNull(handle.getClusters());

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before hosts dumping.", exception.getMessage());

    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturns4xx_throwsCriticalException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    mockClouderaClusterAPIResponse("first-cluster", "[]", HttpStatus.SC_BAD_REQUEST);

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertTrue(
        exception
            .getMessage()
            .contains("Cloudera Error: Response status code is 400 but 2xx is expected."));
  }

  @Test
  public void doRun_clouderaReturns5xx_throwsCriticalException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    mockClouderaClusterAPIResponse("first-cluster", "[]", HttpStatus.SC_INTERNAL_SERVER_ERROR);

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertTrue(
        exception
            .getMessage()
            .contains("Cloudera Error: Response status code is 500 but 2xx is expected."));
  }

  @Test
  public void doRun_clouderaReturnsInvalidJsonFormat_throwsCriticalException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    mockClouderaClusterAPIResponse("first-cluster", "[}");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertTrue(exception.getMessage().contains("Cloudera Error:"));
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private CloseableHttpResponse mockClouderaClusterAPIResponse(
      String clusterName, String itemsJsonLine) throws IOException {
    return mockClouderaClusterAPIResponse(clusterName, itemsJsonLine, HttpStatus.SC_OK);
  }

  private CloseableHttpResponse mockClouderaClusterAPIResponse(
      String clusterName, String itemsJsonLine, int statusCode) throws IOException {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(entity);
    mockStatusLine(response, statusCode);

    when(httpClient.execute(
            argThat(
                (HttpGet get) ->
                    get != null
                        && get.getURI()
                            .toString()
                            .endsWith(String.format("/%s/hosts", clusterName)))))
        .thenReturn(response);

    when(entity.getContent())
        .thenReturn(
            new ByteArrayInputStream(
                String.format("{\"clusterName\":\"%s\",\"items\":%s}", clusterName, itemsJsonLine)
                    .getBytes()));
    return response;
  }

  private void mockStatusLine(CloseableHttpResponse responseHost, int statusCode) {
    StatusLine statusLine = mock();
    when(responseHost.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(statusCode);
  }

  private Set<URI> getCalledURLs() throws IOException {
    Set<URI> requestedUrls = new HashSet<>();
    verify(httpClient, times(2))
        .execute(
            argThat(
                request -> {
                  assertEquals(HttpGet.class, request.getClass());
                  requestedUrls.add(request.getURI());
                  return true;
                }));
    return requestedUrls;
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

  private void verifyNoWrites() throws IOException {
    verify(writer, never()).write(anyChar());
    verify(writer, never()).write(anyString());
    verify(writer, never()).write(anyString(), anyInt(), anyInt());
    verify(writer, never()).write(any(char[].class));
    verify(writer, never()).write(any(char[].class), anyInt(), anyInt());
  }
}
