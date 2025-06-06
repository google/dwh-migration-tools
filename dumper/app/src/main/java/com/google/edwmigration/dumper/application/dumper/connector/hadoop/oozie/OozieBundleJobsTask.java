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

import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.XOozieClient;

public class OozieBundleJobsTask extends AbstractOozieJobsTask<BundleJob> {

  public OozieBundleJobsTask(ZonedDateTime startDate, ZonedDateTime endDate) {
    super("oozie_bundle_jobs.csv", startDate, endDate);
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  boolean isInDateRange(BundleJob job, long minJobEndTimeTimestamp, long maxJobEndTimeTimestamp) {
    Date jobEndTime = getJobEndTime(job);
    // Bundle's endTime is obtained from Coordinators under the bundle control,
    // so the similar logic is applied.
    if (jobEndTime == null) {
      return job.getStartTime().getTime() < maxJobEndTimeTimestamp;
    } else {
      return minJobEndTimeTimestamp <= jobEndTime.getTime()
          && jobEndTime.getTime() < maxJobEndTimeTimestamp;
    }
  }

  @Override
  List<BundleJob> fetchJobsWithFilter(
      XOozieClient oozieClient, String oozieFilter, int start, int len)
      throws OozieClientException {
    return oozieClient.getBundleJobsInfo(oozieFilter, start, len);
  }

  @Override
  Date getJobEndTime(BundleJob job) {
    return job.getEndTime();
  }
}
