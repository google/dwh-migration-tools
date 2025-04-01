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
import java.lang.reflect.ParameterizedType;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.XOozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOozieJobsTask<J> extends AbstractTask<Void> {
  private static final Logger logger = LoggerFactory.getLogger(AbstractOozieJobsTask.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final int maxDaysToFetch;
  private final long initialTimestamp;
  private final Class<J> oozieJobClass;

  AbstractOozieJobsTask(String fileName, int maxDaysToFetch, long initialTimestamp) {
    super(fileName);
    Preconditions.checkArgument(
        maxDaysToFetch >= 1,
        String.format("Amount of days must be a positive number. Got %d.", maxDaysToFetch));

    this.maxDaysToFetch = maxDaysToFetch;
    this.initialTimestamp = initialTimestamp;
    this.oozieJobClass =
        (Class<J>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    final CSVFormat csvFormat = newCsvFormatForClass(oozieJobClass);
    final ImmutableList<String> csvHeader = ImmutableList.copyOf(csvFormat.getHeader());

    try (CSVPrinter printer = csvFormat.print(sink.asCharSink(UTF_8).openBufferedStream())) {
      XOozieClient oozieClient = ((OozieHandle) handle).getOozieClient();
      final int batchSize = context.getArguments().getPaginationPageSize();
      int offset = 0;
      long lastJobEndTimestamp = initialTimestamp;

      logger.info(
          "Start fetching Oozie jobs for type {} for the last {}d starting from {}ms",
          oozieJobClass.getSimpleName(),
          maxDaysToFetch,
          initialTimestamp);

      while (initialTimestamp - lastJobEndTimestamp < TimeUnit.DAYS.toMillis(maxDaysToFetch)) {
        List<J> jobs = fetchJobs(oozieClient, null, null, offset, batchSize);
        for (J job : jobs) {
          Object[] record = toCSVRecord(job, csvHeader);
          printer.printRecord(record);
        }

        if (jobs.isEmpty()) {
          break;
        }

        J lastJob = jobs.get(jobs.size() - 1);
        Date endTime = getJobEndDateTime(lastJob);
        if (endTime == null) {
          break;
        }
        lastJobEndTimestamp = endTime.getTime();
        offset += jobs.size();
      }

      printer.println();
    }
    return null;
  }

  // todo jobs params in filter
  //        FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_START);
  //         FILTER_NAMES.add(OozieClient.FILTER_CREATED_TIME_END);
  abstract List<J> fetchJobs(
      XOozieClient oozieClient, Date startDate, Date endDate, int start, int len)
      throws OozieClientException;

  abstract Date getJobEndDateTime(J job);

  private static Object[] toCSVRecord(Object job, ImmutableList<String> header) throws Exception {
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
