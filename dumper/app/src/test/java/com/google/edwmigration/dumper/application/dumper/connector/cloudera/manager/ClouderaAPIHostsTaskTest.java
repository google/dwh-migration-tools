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
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.MockUtils.getWrittenJsonLines;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.MockUtils.verifyNoWrites;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.TestUtils.readFileAsString;
import static com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.TestUtils.toJsonl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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

  private static WireMockServer server;
  private final ClouderaAPIHostsTask task = new ClouderaAPIHostsTask();
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
  public void doRun_clouderaReturnsValidJsonLines_writeJsonLines() throws Exception {
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id125", "second-cluster"));
    String hostsJson = readFileAsString("/cloudera/manager/api-hosts.json");
    stubClouderaClusterAPIResponse("first-cluster", hostsJson);
    stubClouderaClusterAPIResponse("second-cluster", "{\"items\":[]}\r\n");

    task.doRun(context, sink, handle);

    List<String> fileLines = getWrittenJsonLines(writer, 2);
    assertEquals(ImmutableList.of(toJsonl(hostsJson), "{\"items\":[]}"), fileLines);
    assertEquals(
        ImmutableList.of(
            ClouderaHostDTO.create("1", "first-host"), ClouderaHostDTO.create("2", "second-host")),
        handle.getHosts());
    verify(writer, times(2)).write('\n');
    verify(writer).close();
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsCriticalException() throws Exception {
    assertNull(handle.getClusters());

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before hosts dumping.", exception.getMessage());

    verifyNoWrites(writer);
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

  @Test
  public void doRun_clouderaReturnsNoHosts_throwsException() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubClouderaClusterAPIResponse("first-cluster", "{\"items\":[]}");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "No hosts were found in any of the initialized Cloudera clusters.", exception.getMessage());
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubClouderaClusterAPIResponse(String clusterName, String responseJson)
      throws IOException {
    stubClouderaClusterAPIResponse(clusterName, responseJson, HttpStatus.SC_OK);
  }

  private void stubClouderaClusterAPIResponse(
      String clusterName, String responseJson, int statusCode) {
    server.stubFor(
        get(urlMatching(String.format(".*/%s/hosts.*", clusterName)))
            .willReturn(okJson(responseJson).withStatus(statusCode)));
  }
}
