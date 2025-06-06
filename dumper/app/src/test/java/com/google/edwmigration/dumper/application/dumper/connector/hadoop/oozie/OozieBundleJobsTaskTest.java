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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.XOozieClient;
import org.junit.Test;

public class OozieBundleJobsTaskTest {
  private final OozieBundleJobsTask task =
      new OozieBundleJobsTask(ZonedDateTime.now().minusDays(1), ZonedDateTime.now());

  @Test
  public void fileName() {
    assertEquals("oozie_bundle_jobs.csv", task.getTargetPath());
  }

  @Test
  public void fetchJobs_success() throws Exception {
    XOozieClient oozieClient = mock(XOozieClient.class);

    // Act
    task.fetchJobsWithFilter(oozieClient, "some;filter", 2, 14);

    // Verify
    verify(oozieClient).getBundleJobsInfo("some;filter", 2, 14);
    verifyNoMoreInteractions(oozieClient);
  }

  @Test
  public void getJobEndTime_success() throws Exception {
    BundleJob job = mock(BundleJob.class);

    // Act
    task.getJobEndTime(job);

    // Verify
    verify(job).getEndTime();
    verifyNoMoreInteractions(job);
  }

  @Test
  public void isInDateRange_endDateIsExcluded() {
    BundleJob job = mock(BundleJob.class);

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
    BundleJob job = mock(BundleJob.class);
    when(job.getEndTime()).thenReturn(null);

    when(job.getStartTime()).thenReturn(new Date(2L));
    assertTrue(
        "endTime is null, so Coordinators with start time before end should be included",
        task.isInDateRange(job, 4L, 6L));

    when(job.getStartTime()).thenReturn(new Date(5L));
    assertTrue(
        "endTime is null, so Coordinators with start time before end should be included",
        task.isInDateRange(job, 4L, 6L));

    when(job.getStartTime()).thenReturn(new Date(7L));
    assertFalse(
        "endTime is null, so Coordinators with start time after end should not be included",
        task.isInDateRange(job, 4L, 6L));
  }

  @Test
  public void getJobEndTime_nullable() {
    BundleJob job = mock(BundleJob.class);

    assertNull("null is expected value for end time", task.getJobEndTime(job));
  }

  @Test
  public void csvHeadersContainRequiredFields() {
    String[] header = task.createJobSpecificCSVFormat().getHeader();
    Arrays.sort(header);

    String[] expected = {
      "acl",
      "appName",
      "appPath",
      "conf",
      "consoleUrl",
      "coordinators",
      "createdTime",
      "endTime",
      "externalId",
      "group",
      "id",
      "kickoffTime",
      "pauseTime",
      "startTime",
      "status",
      "timeUnit",
      "timeout",
      "user"
    };
    assertArrayEquals(expected, header);
  }
}
