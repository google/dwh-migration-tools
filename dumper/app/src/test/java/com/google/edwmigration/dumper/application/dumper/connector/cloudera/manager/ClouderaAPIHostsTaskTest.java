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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
public class ClouderaAPIHostsTaskTest {

  private final ClouderaAPIHostsTask task = new ClouderaAPIHostsTask();
  private ClouderaManagerHandle handle;
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
    URI uri = URI.create(server.baseUrl() + "/api/vTest");
    handle = new ClouderaManagerHandle(uri, HttpClients.createDefault());

    when(sink.asCharSink(eq(StandardCharsets.UTF_8))).thenReturn(charSink);
    when(charSink.openBufferedStream()).thenReturn(writer);
  }

  @Test
  public void doRun_clouderaReturnsValidJsonLines_writeJsonLines() throws Exception {
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id125", "second-cluster"));

    stubClouderaClusterAPIResponse("first-cluster", "[]");
    stubClouderaClusterAPIResponse("second-cluster", "[]\r\n");

    task.doRun(context, sink, handle);

    // write jsonl. https://jsonlines.org/
    Set<String> fileLines = getWrittenJsonLines();
    verify(writer, times(2)).write('\n');
    assertEquals(
        ImmutableSet.of(
            "{\"clusterName\":\"first-cluster\",\"items\":[]}",
            "{\"clusterName\":\"second-cluster\",\"items\":[]}"),
        fileLines);

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
  public void doRun_clouderaReturns4xx_throwsException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubClouderaClusterAPIResponse("first-cluster", "[]", HttpStatus.SC_BAD_REQUEST);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    assertTrue(
        exception
            .getMessage()
            .contains("Cloudera Error: Response status code is 400 but 2xx is expected."));
  }

  @Test
  public void doRun_clouderaReturns5xx_throwsException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubClouderaClusterAPIResponse("first-cluster", "[]", HttpStatus.SC_INTERNAL_SERVER_ERROR);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    assertTrue(
        exception
            .getMessage()
            .contains("Cloudera Error: Response status code is 500 but 2xx is expected."));
  }

  @Test
  public void doRun_clouderaReturnsInvalidJsonFormat_throwsException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubClouderaClusterAPIResponse("first-cluster", "[}");

    assertThrows(JsonParseException.class, () -> task.doRun(context, sink, handle));
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubClouderaClusterAPIResponse(String clusterName, String itemsJsonLine)
      throws IOException {
    stubClouderaClusterAPIResponse(clusterName, itemsJsonLine, HttpStatus.SC_OK);
  }

  private void stubClouderaClusterAPIResponse(
      String clusterName, String itemsJsonLine, int statusCode) throws IOException {
    String clusterAPIResponse =
        String.format("{\"clusterName\":\"%s\",\"items\":%s}", clusterName, itemsJsonLine);
    server.stubFor(
        get(urlMatching(String.format(".*/%s/hosts.*", clusterName)))
            .willReturn(okJson(clusterAPIResponse).withStatus(statusCode)));
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
