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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.lang.reflect.ParameterizedType;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.XOozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOozieJobsTask<J> extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractOozieJobsTask.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final DateTimeFormatter ISO8601_UTC_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'").withZone(ZoneOffset.UTC);
  private static final int INITIAL_OOZIE_JOBS_OFFSET = 1; // starts with 1, not 0.
  private static final String SORT_BY_END_TIME = OozieClient.FILTER_SORT_BY + "=endTime;";

  private final ZonedDateTime startDate;
  private final ZonedDateTime endDate;
  private final Class<J> oozieJobClass;

  AbstractOozieJobsTask(String fileName, ZonedDateTime startDate, ZonedDateTime endDate) {
    super(fileName);
    Preconditions.checkArgument(
        startDate.isBefore(endDate),
        "Start date [%s] must be before end date [%s].",
        startDate,
        endDate);

    this.startDate = startDate;
    this.endDate = endDate;
    this.oozieJobClass =
        (Class<J>)
            ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    final CSVFormat csvFormat = createJobSpecificCSVFormat();
    final ImmutableList<String> csvHeader = ImmutableList.copyOf(csvFormat.getHeader());

    try (CSVPrinter printer = csvFormat.print(sink.asCharSink(UTF_8).openBufferedStream())) {
      XOozieClient oozieClient = ((OozieHandle) handle).getOozieClient();
      final int batchSize = context.getArguments().getPaginationPageSize();
      final long minJobEndTimeTimestamp = startDate.toInstant().toEpochMilli();
      final long maxJobEndTimeTimestamp = endDate.toInstant().toEpochMilli();

      int offset = INITIAL_OOZIE_JOBS_OFFSET;
      long latestFetchedJobEndTimestamp = maxJobEndTimeTimestamp; // start iteration from end time

      logger.info(
          "Start fetching Oozie jobs for type {} from [{}] to [{}] by {} with client side filtering",
          oozieJobClass.getSimpleName(),
          toISO(startDate),
          toISO(endDate),
          batchSize);

      while (latestFetchedJobEndTimestamp >= minJobEndTimeTimestamp) {
        List<J> jobs = fetchJobsWithFilter(oozieClient, SORT_BY_END_TIME, offset, batchSize);
        for (J job : jobs) {
          Date currentJobEndTime = getJobEndTime(job);
          boolean inDateRange =
              currentJobEndTime != null
                  && minJobEndTimeTimestamp <= currentJobEndTime.getTime()
                  && currentJobEndTime.getTime() < maxJobEndTimeTimestamp;
          if (!inDateRange) {
            // It's client side filtering. It's inefficient.
            // Unfortunately  Oozie doesn't provide an API to filter by start/end date.
            // It's possible to filter by OozieClient.FILTER_CREATED_TIME_START
            // but a job can be created 25 years and still be executed each day or minute.
            // So, job creation time is not what is really needed.
            continue;
          }
          Object[] record = toCSVRecord(job, csvHeader);
          printer.printRecord(record);
        }

        if (jobs.isEmpty()) {
          break;
        }

        J lastJob = jobs.get(jobs.size() - 1);
        Date endTime = getJobEndTime(lastJob);
        boolean isLastJobInProgress = endTime == null;
        if (!isLastJobInProgress) {
          latestFetchedJobEndTimestamp = endTime.getTime();
        }
        offset += jobs.size();
      }

      printer.println();
    }
    return null;
  }

  CSVFormat createJobSpecificCSVFormat() {
    return newCsvFormatForClass(oozieJobClass);
  }

  /**
   * Method is expected to do a call to Oozie server to fetch jobs in a ranger [{@code startDate} -
   * {@code endDate}].
   *
   * @param oozieClient - Oozie client initialised to particular Oozie server
   * @param start jobs offset pass to {@code oozieClient}
   * @param len number of jobs to return
   */
  abstract List<J> fetchJobsWithFilter(
      XOozieClient oozieClient, String oozieFilter, int start, int len) throws OozieClientException;

  @Nullable
  abstract Date getJobEndTime(J job);

  private static String toISO(ZonedDateTime dateTime) {
    return ISO8601_UTC_FORMAT.format(dateTime.toInstant());
  }

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

  @VisibleForTesting
  ZonedDateTime getStartDate() {
    return startDate;
  }

  @VisibleForTesting
  ZonedDateTime getEndDate() {
    return endDate;
  }
}
