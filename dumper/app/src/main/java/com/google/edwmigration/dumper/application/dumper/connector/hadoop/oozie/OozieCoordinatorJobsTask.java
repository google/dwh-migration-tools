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

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.XOozieClient;

public class OozieCoordinatorJobsTask extends AbstractOozieJobsTask<CoordinatorJob> {

  public OozieCoordinatorJobsTask(ZonedDateTime startDate, ZonedDateTime endDate) {
    super("oozie_coord_jobs.csv", startDate, endDate);
  }

  @Override
  List<CoordinatorJob> fetchJobsWithFilter(
      XOozieClient oozieClient, String oozieFilter, int start, int len)
      throws OozieClientException {
    return oozieClient.getCoordJobsInfo(oozieFilter, start, len);
  }

  @Override
  Date getJobCreatedTime(CoordinatorJob job) {
    return job.getCreatedTime();
  }
}
