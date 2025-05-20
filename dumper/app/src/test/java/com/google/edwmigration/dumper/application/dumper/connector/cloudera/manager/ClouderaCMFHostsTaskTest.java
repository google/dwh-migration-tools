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

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
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
public class ClouderaCMFHostsTaskTest {

  private final ClouderaCMFHostsTask task = new ClouderaCMFHostsTask();

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
  public void doRun_clouderaReturnsValidJson_writeJsonLines() throws Exception {
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id34", "next-cluster"));

    stubCMFHostResponse("id1", "first-cluster", "[]");
    stubCMFHostResponse("id34", "next-cluster", "[]\n\r");

    task.doRun(context, sink, handle);

    // write jsonl. https://jsonlines.org/
    Set<String> fileLines = getWrittenJsonLines();
    verify(writer, times(2)).write('\n');
    assertEquals(
        ImmutableSet.of(
            "{\"clusterName\":\"first-cluster\",\"hosts\":[]}",
            "{\"clusterName\":\"next-cluster\",\"hosts\":[]}"),
        fileLines);
    verify(writer).close();
  }

  @Test
  public void doRun_clouderaReturnsNoHostForCluster_throwsWarningException() throws Exception {
    // GIVEN: The cluster which has no host
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    String mockedResponse = String.format("{\"clusterName\" :\"%s\"}", "first-cluster");
    server.stubFor(
        get(urlMatching("/cmf/hardware/hosts/hostsOverview.json\\?clusterId=id1.*"))
            .willReturn(okJson(mockedResponse).withStatus(HttpStatus.SC_OK)));

    // WHEN: Hosts are requested from the API and no one has been returned
    MismatchedInputException exception =
        assertThrows(MismatchedInputException.class, () -> task.doRun(context, sink, handle));

    // THEN: The exception has to be raised
    assertTrue(exception.getMessage().contains("hosts"));
  }

  @Test
  public void doRun_initClusterWithoutId_skipWrites() throws Exception {
    initClusters(ClouderaClusterDTO.create(null, "single cluster"));

    task.doRun(context, sink, handle);

    verifyNoWrites();
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsException() throws Exception {
    assertNull(handle.getClusters());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before hosts dumping.", exception.getMessage());
    verifyNoWrites();
  }

  @Test
  public void doRun_clouderaReturnsInvalidJson_continueTaskWithoutWriting() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubCMFHostResponse("id1", "first-cluster", "[}");
    verifyNoWrites();
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubCMFHostResponse(String clusterId, String clusterName, String jsonHosts) {
    String cmfResponse =
        String.format("{\"clusterName\" :\"%s\", \"hosts\": %s}", clusterName, jsonHosts);
    server.stubFor(
        get(urlMatching(
                String.format(
                    "/cmf/hardware/hosts/hostsOverview\\.json\\?clusterId=%s.*", clusterId)))
            .willReturn(okJson(cmfResponse)));
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
