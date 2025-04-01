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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import com.github.tomakehurst.wiremock.verification.FindRequestsResult;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.JsonUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OozieWorkflowJobsTaskTest {
  private static final long timestampInMockResponses = 1742220060000L;
  private static WireMockServer server;

  @Mock private TaskRunContext context;
  private AuthOozieClient oozieClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    server =
        new WireMockServer(
            WireMockConfiguration.wireMockConfig()
                .dynamicPort()
                .extensions(new ResponseTemplateTransformer(true)));
    server.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    server.stop();
  }

  @Before
  public void setUp() throws Exception {
    server.resetAll();
    oozieClient = new TestNoAuthOozieClient(server.baseUrl(), null);
  }

  @Test
  public void doRun_requestBatchesUntilAllDaysFetched_ok() throws Exception {
    when(context.getArguments()).thenReturn(new ConnectorArguments("--connector", "oozie"));
    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=-sortby+endTime&jobtype=wf&offset=0&len=1000"))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));

    final int maxDaysToFetch = 4;
    Date lastCapturedDate =
        new Date(timestampInMockResponses - TimeUnit.DAYS.toMillis(maxDaysToFetch));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=-sortby+endTime&jobtype=wf&offset=3&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        "endDateParam", JsonUtils.formatDateRfc822(lastCapturedDate))));

    OozieWorkflowJobsTask task =
        new OozieWorkflowJobsTask(maxDaysToFetch, timestampInMockResponses);

    // Act
    try {
      task.doRun(context, sink, new OozieHandle(oozieClient));
    } finally {
      FindRequestsResult unmatchedRequests = server.findUnmatchedRequests();
      System.out.println(unmatchedRequests);
    }

    // Assert
    String actual = sink.getContent();
    String expected = readFileAsString("/oozie/expected-jobs-byenddate.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void doRun_requestBatchesUntilEmptyResponse_ok() throws Exception {
    when(context.getArguments()).thenReturn(new ConnectorArguments("--connector", "oozie"));

    testWithBatchSize(1000);
  }

  @Test
  public void doRun_requestBatchesUntilEmptyResponseByCustomBatch_ok() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "oozie", "--pagination-page-size", "42");
    when(context.getArguments()).thenReturn(arguments);

    testWithBatchSize(42);
  }

  private void testWithBatchSize(int batchSize) throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=-sortby+endTime&jobtype=wf&offset=0&len=" + batchSize))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=-sortby+endTime&jobtype=wf&offset=3&len=" + batchSize))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch2.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=-sortby+endTime&jobtype=wf&offset=4&len=" + batchSize))
            .willReturn(okJson("{}")));

    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(10, timestampInMockResponses);

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = sink.getContent();
    String expected = readFileAsString("/oozie/expected-jobs.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void create_nonpositiveDays_throwsException() throws Exception {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new OozieWorkflowJobsTask(0));

    assertEquals("Amount of days must be a positive number. Got 0.", exception.getMessage());

    exception = assertThrows(IllegalArgumentException.class, () -> new OozieWorkflowJobsTask(-3));

    assertEquals("Amount of days must be a positive number. Got -3.", exception.getMessage());

    new OozieWorkflowJobsTask(1);
  }

  @Test
  public void fileName() {
    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(10);

    assertEquals("oozie_workflow_jobs.csv", task.getTargetPath());
  }

  @Test
  public void fetchJobs_success() throws Exception {
    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(10);
    XOozieClient oozieClient = mock(XOozieClient.class);

    // Act
    task.fetchJobs(oozieClient, null, null, 3, 17);

    // Verify
    verify(oozieClient).getJobsInfo("-sortby endTime", 3, 17);
    verifyNoMoreInteractions(oozieClient);
  }

  @Test
  public void getJobEndTime_success() throws Exception {
    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(10);
    WorkflowJob job = mock(WorkflowJob.class);

    // Act
    task.getJobEndDateTime(job);

    // Verify
    verify(job).getEndTime();
    verifyNoMoreInteractions(job);
  }

  private static void stubOozieVersionsCall() {
    server.stubFor(
        options(urlEqualTo("/versions")).willReturn(okJsonWithBodyFile("oozie/versions.json")));
    server.stubFor(
        get(urlEqualTo("/versions")).willReturn(okJsonWithBodyFile("oozie/versions.json")));
  }

  private static ResponseDefinitionBuilder okJsonWithBodyFile(String fileName) {
    return ok().withBodyFile(fileName).withHeader(CONTENT_TYPE, "application/json");
  }

  private String readFileAsString(String fileName) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(this.getClass().getResource(fileName).toURI())));
  }
}
