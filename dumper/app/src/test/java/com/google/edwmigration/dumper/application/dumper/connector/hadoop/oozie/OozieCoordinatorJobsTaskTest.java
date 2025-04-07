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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Arrays;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.XOozieClient;
import org.junit.Test;

public class OozieCoordinatorJobsTaskTest {
  private final OozieCoordinatorJobsTask task = new OozieCoordinatorJobsTask(5);

  @Test
  public void fileName() {
    assertEquals("oozie_coord_jobs.csv", task.getTargetPath());
  }

  @Test
  public void fetchJobs_success() throws Exception {
    XOozieClient oozieClient = mock(XOozieClient.class);

    // Act
    task.fetchJobs(oozieClient, null, null, 2, 14);

    // Verify
    verify(oozieClient).getCoordJobsInfo("sortby=endTime;", 2, 14);
    verifyNoMoreInteractions(oozieClient);
  }

  @Test
  public void getJobEndTime_success() throws Exception {
    CoordinatorJob job = mock(CoordinatorJob.class);

    // Act
    task.getJobEndDateTime(job);

    // Verify
    verify(job).getEndTime();
    verifyNoMoreInteractions(job);
  }

  @Test
  public void csvHeadersContainRequiredFields() {
    String[] header = task.createJobSpecificCSVFormat().getHeader();
    Arrays.sort(header);

    String[] expected =
        new String[] {
          "acl",
          "actions",
          "appName",
          "appPath",
          "bundleId",
          "concurrency",
          "conf",
          "consoleUrl",
          "createdTime",
          "endTime",
          "executionOrder",
          "externalId",
          "frequency",
          "group",
          "id",
          "lastActionTime",
          "nextMaterializedTime",
          "pauseTime",
          "startTime",
          "status",
          "timeUnit",
          "timeZone",
          "timeout",
          "user"
        };
    assertArrayEquals(expected, header);
  }
}
