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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OozieJobsTask extends AbstractTask<Void> {
  private static final Logger logger = LoggerFactory.getLogger(OozieJobsTask.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final int maxDaysToFetch;
  private final long initialTimestamp;

  public OozieJobsTask(int maxDaysToFetch) {
    this(maxDaysToFetch, System.currentTimeMillis());
  }

  OozieJobsTask(int maxDaysToFetch, long initialTimestamp) {
    super("oozie_jobs.csv");
    Preconditions.checkArgument(
        maxDaysToFetch >= 1,
        String.format("Amount of days must be a positive number. Got %d.", maxDaysToFetch));

    this.maxDaysToFetch = maxDaysToFetch;
    this.initialTimestamp = initialTimestamp;
  }

  // todo jobs params in filter
  //        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_START);
  //         FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_END);
  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    final CSVFormat csvFormat = newCsvFormatForClass(WorkflowJob.class);
    final ImmutableList<String> csvHeader = ImmutableList.copyOf(csvFormat.getHeader());

    try (CSVPrinter printer = csvFormat.print(sink.asCharSink(UTF_8).openBufferedStream())) {
      XOozieClient oozieClient = ((OozieHandle) handle).getOozieClient();
      final int batchSize = context.getArguments().getPaginationPageSize();
      int offset = 0;
      long lastJobEndTimestamp = initialTimestamp;

      logger.info(
          "Start fetch Oozie jobs for last {}d starts from {}ms", maxDaysToFetch, initialTimestamp);

      while (initialTimestamp - lastJobEndTimestamp < TimeUnit.DAYS.toMillis(maxDaysToFetch)) {
        List<WorkflowJob> jobsInfo = oozieClient.getJobsInfo(null, offset, batchSize);
        for (WorkflowJob workflowJob : jobsInfo) {
          Object[] record = toCSVRecord(workflowJob, csvHeader);
          printer.printRecord(record);
        }

        if (jobsInfo.isEmpty()) {
          break;
        }

        WorkflowJob lastJob = jobsInfo.get(jobsInfo.size() - 1);
        if (lastJob.getEndTime() == null) {
          break;
        }
        lastJobEndTimestamp = lastJob.getEndTime().getTime();
        offset += jobsInfo.size();
      }

      printer.println();
    }
    return null;
  }

  private static Object[] toCSVRecord(WorkflowJob job, ImmutableList<String> header)
      throws Exception {
    Object[] record = new Object[header.size()];
    for (int i = 0; i < header.size(); i++) {
      record[i] = PropertyUtils.getProperty(job, header.get(i));
      if (record[i] != null && record[i] instanceof Date) {
        // avoid date formats complexity and use milliseconds
        record[i] = ((Date) record[i]).getTime();
      }
      if (record[i] != null && record[i] instanceof List) {
        // write Actions arrays as json string in csv
        record[i] = objectMapper.writeValueAsString(record[i]);
      }
    }
    return record;
  }
}
