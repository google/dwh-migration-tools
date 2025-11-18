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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.AbstractClouderaTimeSeriesTask.TimeSeriesAggregation;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
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
public class ClouderaHostRAMChartTaskTest {
  private final ClouderaHostRAMChartTask task =
      new ClouderaHostRAMChartTask(
          timeTravelDaysAgo(1),
          timeTravelDaysAgo(0),
          TimeSeriesAggregation.HOURLY,
          TaskCategory.REQUIRED);
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
  public void doRun_hostsExists_success() throws Exception {
    initHosts(
        ClouderaHostDTO.create("id1", "first-host"),
        ClouderaHostDTO.create("id125", "second-host"));
    stubHostAPIResponse("id1", HttpStatus.SC_OK, "{\"items\":[\"host1\"]}");
    stubHostAPIResponse("id125", HttpStatus.SC_OK, "{\n\"items\":[\"host2\"]\n\r}");

    task.doRun(context, sink, handle);

    Set<String> fileLines = new HashSet<>(MockUtils.getWrittenJsonLines(writer, 2));
    verify(writer, times(2)).write('\n');
    assertEquals(ImmutableSet.of("{\"items\":[\"host1\"]}", "{\"items\":[\"host2\"]}"), fileLines);
    verify(writer).close();
  }

  @Test
  public void doRun_clustersWereNotInitialized_throwsCriticalException() throws Exception {
    assertNull(handle.getClusters());

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> task.doRun(context, sink, handle));

    assertEquals(
        "Cloudera hosts must be initialized before RAM charts dumping.", exception.getMessage());

    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaServerReturnsInvalidJson_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "first-host"));
    stubHostAPIResponse("id1", HttpStatus.SC_OK, "\"items\":[\"host1\"]}");

    assertThrows(JsonParseException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaServerReturns4xx_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "first-host"));
    stubHostAPIResponse("id1", HttpStatus.SC_BAD_REQUEST, "{\"items\":[\"host1\"]}");

    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites(writer);
  }

  @Test
  public void doRun_clouderaServerReturns5xx_throwsException() throws Exception {
    initHosts(ClouderaHostDTO.create("id1", "first-host"));
    stubHostAPIResponse("id1", HttpStatus.SC_INTERNAL_SERVER_ERROR, "{\"items\":[\"host1\"]}");

    assertThrows(RuntimeException.class, () -> task.doRun(context, sink, handle));

    verifyNoWrites(writer);
  }

  private void initHosts(ClouderaHostDTO... hosts) {
    handle.initHosts(Arrays.asList(hosts));
  }

  private void stubHostAPIResponse(String hostId, int statusCode, String responseContent)
      throws IOException {
    server.stubFor(
        get(urlMatching(String.format("/api/vTest/timeseries.*%s.*", hostId)))
            .willReturn(okJson(responseContent).withStatus(statusCode)));
  }

  private ZonedDateTime timeTravelDaysAgo(int days) {
    ZonedDateTime today = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("UTC"));
    return today.minusDays(days);
  }
}
