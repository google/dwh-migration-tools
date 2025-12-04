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
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
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
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaServicesTaskTest {
  private static WireMockServer server;

  private final ClouderaServicesTask task = new ClouderaServicesTask();

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
  public void clustersExists_do_writes_success() throws Exception {
    String servicesJson = readFileAsString("/cloudera/manager/cluster-status.json");
    handle.initClusters(
        ImmutableList.of(
            ClouderaClusterDTO.create("id1", "first-cluster"),
            ClouderaClusterDTO.create("id25", "second-cluster")));

    server.stubFor(
        get("/api/vTest/clusters/first-cluster/services").willReturn(okJson(servicesJson)));
    server.stubFor(
        get("/api/vTest/clusters/second-cluster/services")
            .willReturn(okJson("{\"some\": \n \"json\" \r}")));

    // Act
    task.doRun(context, sink, handle);

    // Assert
    server.verify(getRequestedFor(urlEqualTo("/api/vTest/clusters/first-cluster/services")));
    server.verify(getRequestedFor(urlEqualTo("/api/vTest/clusters/second-cluster/services")));

    verify(writer).write(toJsonl(servicesJson));
    verify(writer).write("{\"some\":\"json\"}");
    verify(writer, times(2)).write('\n');
    verify(writer).close();
  }

  @Test
  public void clustersWereNotInitialized_throwsException() throws Exception {
    assertNull(handle.getClusters());

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera clusters must be initialized before services dumping.", exception.getMessage());

    verifyNoWrites(writer);
  }
}
