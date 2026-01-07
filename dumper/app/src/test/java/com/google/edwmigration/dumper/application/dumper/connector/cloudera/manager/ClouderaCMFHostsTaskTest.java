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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.common.io.CharSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
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

  private static WireMockServer server;
  private final ClouderaCMFHostsTask task = new ClouderaCMFHostsTask();
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
  public void doRun_clouderaReturnsValidJson_writeJsonLines() throws Exception {
    initClusters(
        ClouderaClusterDTO.create("id1", "first-cluster"),
        ClouderaClusterDTO.create("id34", "next-cluster"));
    String hostsJson = readFileAsString("/cloudera/manager/cmf-hosts.json");
    stubCMFHostResponse("id1", hostsJson);
    stubCMFHostResponse("id34", "{\"hosts\":[]}\n\r");

    task.doRun(context, sink, handle);

    List<String> fileLines = getWrittenJsonLines(writer, 2);
    assertEquals(ImmutableList.of(toJsonl(hostsJson), "{\"hosts\":[]}"), fileLines);
    verify(writer, times(2)).write('\n');
    verify(writer).close();
  }

  @Test
  public void doRun_initClusterWithoutId_skipWrites() throws Exception {
    initClusters(ClouderaClusterDTO.create(null, "single cluster"));

    task.doRun(context, sink, handle);

    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsException() throws Exception {
    assertNull(handle.getClusters());

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before hosts dumping.", exception.getMessage());
    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaReturnsInvalidJson_continueTaskWithoutWriting() throws Exception {
    initClusters(ClouderaClusterDTO.create("id1", "first-cluster"));
    stubCMFHostResponse("id1", "[}");

    task.doRun(context, sink, handle);

    verifyNoWrites(writer);
  }

  private void initClusters(ClouderaClusterDTO... clusters) {
    handle.initClusters(Arrays.asList(clusters));
  }

  private void stubCMFHostResponse(String clusterId, String responseJson) {
    server.stubFor(
        get(urlMatching(
                String.format(
                    "/cmf/hardware/hosts/hostsOverview\\.json\\?clusterId=%s.*", clusterId)))
            .willReturn(okJson(responseJson)));
  }
}
