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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.options;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.oozie.client.AuthOozieClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OozieJobsTaskTest {
  private static WireMockServer server;
  private final OozieJobsTask task = new OozieJobsTask(10);

  @Mock private TaskRunContext context;

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
  }

  @Test
  public void doRun_requestBatchesUntilEmptyResponse_ok() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    AuthOozieClient oozieClient = new TestNoAuthOozieClient(server.baseUrl(), null);
    server.stubFor(
        options(urlEqualTo("/versions")).willReturn(okJsonWithBodyFile("oozie/versions.json")));
    server.stubFor(
        get(urlEqualTo("/versions")).willReturn(okJsonWithBodyFile("oozie/versions.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?jobtype=wf&offset=0&len=1000"))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?jobtype=wf&offset=3&len=1000"))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch2.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?jobtype=wf&offset=4&len=1000")).willReturn(okJson("{}")));

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = sink.getContent();
    String expected = readFileAsString("/oozie/expected-jobs.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void create_negativeDays_throwsException() throws Exception {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new OozieJobsTask(0));

    assertEquals("Amount of days must be a positive number. Got 0.", exception.getMessage());

    exception = assertThrows(IllegalArgumentException.class, () -> new OozieJobsTask(-3));

    assertEquals("Amount of days must be a positive number. Got -3.", exception.getMessage());

    new OozieJobsTask(1);
  }

  private static ResponseDefinitionBuilder okJsonWithBodyFile(String fileName) {
    return ok().withBodyFile(fileName).withHeader(CONTENT_TYPE, "application/json");
  }

  private String readFileAsString(String fileName) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(fileName).toURI())));
  }
}
