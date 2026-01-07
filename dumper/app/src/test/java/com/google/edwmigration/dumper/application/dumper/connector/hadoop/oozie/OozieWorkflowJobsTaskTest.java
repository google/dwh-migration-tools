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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import javax.annotation.Nonnull;
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
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final DateTimeFormatter ISO8601_UTC_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(ZoneOffset.UTC);
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
  public void doRun_requestBatchesUntilFullDateRangeFetched_ok() throws Exception {
    final ZonedDateTime endTime =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampInMockResponses), UTC).plusHours(1);
    final ZonedDateTime startTime = endTime.minusDays(7);
    Date lastCapturedDate = Date.from(startTime.toInstant());

    when(context.getArguments()).thenReturn(new ConnectorArguments("--connector", "oozie"));
    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=1&len=1000"))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=4&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        "endTime", JsonUtils.formatDateRfc822(lastCapturedDate))));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=5&len=1000"))
            .willReturn(okJson("{}")));

    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(startTime, endTime);

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = getContent(sink);
    String expected = readFileAsString("/oozie/expected-jobs-all-in-range-byenddate.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void doRun_requestBatchesFilterOnClient_ok() throws Exception {
    final ZonedDateTime endTime =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampInMockResponses), UTC).plusHours(1);
    final ZonedDateTime startTime = endTime.minusDays(7);
    Date lastCapturedDate = Date.from(startTime.toInstant());

    when(context.getArguments()).thenReturn(new ConnectorArguments("--connector", "oozie"));
    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=1&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // filter out this job because after endTime
                        "endTime",
                        JsonUtils.formatDateRfc822(
                            Date.from(endTime.plusSeconds(1).toInstant())))));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=2&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // filter out this job because equals endTime
                        "endTime", JsonUtils.formatDateRfc822(Date.from(endTime.toInstant())))));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=3&len=1000"))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=6&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        "endTime", JsonUtils.formatDateRfc822(lastCapturedDate))));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=7&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // filter out this job because before startTime
                        "endTime",
                        JsonUtils.formatDateRfc822(
                            Date.from(startTime.minusSeconds(1).toInstant())))));

    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(startTime, endTime);

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = getContent(sink);
    String expected =
        readFileAsString("/oozie/expected-jobs-filtered-by-range-sorted-byenddate.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void doRun_requestBatchesFilterOnClient_nullDate_success() throws Exception {
    final ZonedDateTime endTime =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampInMockResponses), UTC).plusHours(1);
    final ZonedDateTime startTime = endTime.minusDays(7);
    Date lastCapturedDate = Date.from(startTime.toInstant());
    when(context.getArguments()).thenReturn(new ConnectorArguments("--connector", "oozie"));
    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=1&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // filter out this job because endTime is null (job is in progress)
                        "endTime", null)));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=2&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // include this job
                        "endTime", JsonUtils.formatDateRfc822(lastCapturedDate))));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=3&len=1000"))
            .willReturn(
                okJsonWithBodyFile("oozie/jobs-one-item-template.json")
                    .withTransformers("response-template")
                    .withTransformerParameter(
                        // filter out this job, because out of range
                        // before start
                        "endTime",
                        JsonUtils.formatDateRfc822(
                            Date.from(startTime.minusSeconds(1).toInstant())))));

    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(startTime, endTime);

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = getContent(sink);
    String expected = readFileAsString("/oozie/expected-jobs-one-job-from-template.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void isInDateRange_endDateIsIncluded() {
    OozieWorkflowJobsTask task = mock(OozieWorkflowJobsTask.class);
    when(task.isInDateRange(any(), anyLong(), anyLong())).thenCallRealMethod();
    when(task.getJobEndTime(any())).thenCallRealMethod();

    WorkflowJob job = mock(WorkflowJob.class);

    when(job.getEndTime()).thenReturn(new Date(5L));
    assertTrue(
        "a job with endDate in the range must be included", task.isInDateRange(job, 0L, 10L));
  }

  @Test
  public void isInDateRange_endDateIsExcluded() {
    OozieWorkflowJobsTask task = mock(OozieWorkflowJobsTask.class);
    when(task.isInDateRange(any(), anyLong(), anyLong())).thenCallRealMethod();
    when(task.getJobEndTime(any())).thenCallRealMethod();

    WorkflowJob job = mock(WorkflowJob.class);

    when(job.getEndTime()).thenReturn(new Date(-1L));
    assertFalse(
        "a job with endDate before the range must not be included",
        task.isInDateRange(job, 0L, 10L));

    when(job.getEndTime()).thenReturn(new Date(5L));
    assertFalse("a defined date range must be excluded.", task.isInDateRange(job, 0L, 5L));

    when(job.getEndTime()).thenReturn(new Date(6L));
    assertFalse(
        "a job with endDate after the range must not be included", task.isInDateRange(job, 0L, 5L));
  }

  @Test
  public void isInDateRange_endDateIsNull() {
    OozieWorkflowJobsTask task = mock(OozieWorkflowJobsTask.class);
    when(task.isInDateRange(any(), anyLong(), anyLong())).thenCallRealMethod();
    when(task.getJobEndTime(any())).thenCallRealMethod();

    WorkflowJob job = mock(WorkflowJob.class);

    when(job.getEndTime()).thenReturn(null);
    assertFalse(
        "in progress jobs are not included",
        task.isInDateRange(job, Long.MIN_VALUE, Long.MAX_VALUE));
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
    final ZonedDateTime endTime =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampInMockResponses), UTC).plusHours(1);
    final ZonedDateTime startTime = endTime.minusDays(7);

    MemoryByteSink sink = new MemoryByteSink();
    stubOozieVersionsCall();
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=1&len=" + batchSize))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch1.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=4&len=" + batchSize))
            .willReturn(okJsonWithBodyFile("oozie/jobs-batch2.json")));
    server.stubFor(
        get(urlEqualTo("/v2/jobs?filter=sortby%3DendTime%3B&jobtype=wf&offset=5&len=" + batchSize))
            .willReturn(okJson("{}")));

    OozieWorkflowJobsTask task = new OozieWorkflowJobsTask(startTime, endTime);

    // Act
    task.doRun(context, sink, new OozieHandle(oozieClient));

    // Assert
    String actual = getContent(sink);
    String expected = readFileAsString("/oozie/expected-jobs.csv");
    assertEquals(expected, actual);
  }

  @Test
  public void create_invalidDateRange_throwsException() throws Exception {
    assertThrows(
        NullPointerException.class, () -> new OozieWorkflowJobsTask(ZonedDateTime.now(), null));

    assertThrows(
        NullPointerException.class, () -> new OozieWorkflowJobsTask(null, ZonedDateTime.now()));

    ZonedDateTime date = ZonedDateTime.of(2000, 1, 4, 4, 59, 47, 0, UTC);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new OozieWorkflowJobsTask(date, date));
    assertEquals(
        "Start date [2000-01-04T04:59:47Z[UTC]] must be before end date [2000-01-04T04:59:47Z[UTC]].",
        exception.getMessage());

    newOozieWorkflowJobTaskForOneDay();
  }

  @Test
  public void fileName() {
    OozieWorkflowJobsTask task = newOozieWorkflowJobTaskForOneDay();

    assertEquals("oozie_workflow_jobs.csv", task.getTargetPath());
  }

  @Test
  public void fetchJobs_success() throws Exception {
    OozieWorkflowJobsTask task = newOozieWorkflowJobTaskForOneDay();
    XOozieClient oozieClient = mock(XOozieClient.class);

    // Act
    task.fetchJobsWithFilter(oozieClient, "some;filter", 3, 17);

    // Verify
    verify(oozieClient).getJobsInfo("some;filter", 3, 17);
    verifyNoMoreInteractions(oozieClient);
  }

  @Test
  public void getJobEndTime_success() throws Exception {
    OozieWorkflowJobsTask task = newOozieWorkflowJobTaskForOneDay();
    WorkflowJob job = mock(WorkflowJob.class);

    // Act
    task.getJobEndTime(job);

    // Verify
    verify(job).getEndTime();
    verifyNoMoreInteractions(job);
  }

  @Test
  public void getJobEndTime_nullable() {
    OozieWorkflowJobsTask task = newOozieWorkflowJobTaskForOneDay();
    WorkflowJob job = mock(WorkflowJob.class);

    assertNull("null is expected value for end time", task.getJobEndTime(job));
  }

  @Test
  public void csvHeadersContainRequiredFields() {
    OozieWorkflowJobsTask task = newOozieWorkflowJobTaskForOneDay();
    String[] header = task.createJobSpecificCSVFormat().getHeader();
    Arrays.sort(header);

    String[] expected = {
      "acl",
      "actions",
      "appName",
      "appPath",
      "conf",
      "consoleUrl",
      "createdTime",
      "endTime",
      "externalId",
      "group",
      "id",
      "lastModifiedTime",
      "parentId",
      "run",
      "startTime",
      "status",
      "user"
    };
    assertArrayEquals(expected, header);
  }

  private static OozieWorkflowJobsTask newOozieWorkflowJobTaskForOneDay() {
    return new OozieWorkflowJobsTask(ZonedDateTime.now().minusDays(1), ZonedDateTime.now());
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

  private static String getContent(@Nonnull ByteSink sink) throws IOException {
    return sink.openStream().toString();
  }

  private static String toISO(ZonedDateTime dateTime) {
    return ISO8601_UTC_FORMAT.format(dateTime.toInstant());
  }
}
