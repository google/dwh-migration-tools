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
import static java.time.LocalDateTime.ofInstant;
import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_URL;
import static org.apache.oozie.cli.OozieCLI.OOZIE_RETRY_COUNT;
import static org.apache.oozie.cli.OozieCLI.WS_HEADER_PREFIX;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.oozie.cli.OozieCLIException;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OozieJobsTask extends AbstractTask<Void> {
  private static final Logger LOG = LoggerFactory.getLogger(OozieJobsTask.class);
  private final ObjectMapper objectMapper = new ObjectMapper();

  private final int maxDaysToFetch;

  public OozieJobsTask(int maxDaysToFetch) {
    super("jobs.csv");
    Preconditions.checkArgument(
        maxDaysToFetch >= 1,
        String.format("Amount of days must be a positive number. Got %d.", maxDaysToFetch));

    this.maxDaysToFetch = maxDaysToFetch;
  }

  @CheckForNull
  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    final LocalDateTime now = LocalDateTime.now();
    final CSVFormat csvFormat = newCsvFormatForClass(WorkflowJob.class);
    final ImmutableList<String> csvHeader = ImmutableList.copyOf(csvFormat.getHeader());

    try (CSVPrinter printer = csvFormat.print(sink.asCharSink(UTF_8).openBufferedStream())) {
      XOozieClient oozieClient = createXOozieClient();
      final int batchSize = 500;
      int offset = 0;
      LocalDateTime lastJobEndDate = now;

      LOG.info("Start fetch Oozie jobs for last {}d starts from {}", maxDaysToFetch, now);

      while (ChronoUnit.DAYS.between(now, lastJobEndDate) < maxDaysToFetch) {
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
        lastJobEndDate = ofInstant(lastJob.getEndTime().toInstant(), ZoneId.systemDefault());
        offset += jobsInfo.size();
      }

      printer.println();
    }
    return null;
  }

  private Object[] toCSVRecord(WorkflowJob job, ImmutableList<String> header) throws Exception {
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

  protected static XOozieClient createXOozieClient() throws OozieCLIException {
    String oozieUrl = getOozieUrl();
    // String authOption = getAuthOption(commandLine);
    XOozieClient wc = new AuthOozieClient(oozieUrl, null);

    addHeaders(wc);
    setRetryCount(wc);
    return wc;
  }

  protected static String getOozieUrl() {
    String url = null; // commandLine.getOptionValue(OOZIE_OPTION);
    if (url == null) {
      url = System.getenv(ENV_OOZIE_URL);
      if (url == null) {
        throw new IllegalArgumentException(
            "Oozie URL is not available neither in command option or in the environment");
      }
    }
    return url;
  }

  protected static void addHeaders(OozieClient wc) {
    // String username = commandLine.getOptionValue(USERNAME);
    // String password = commandLine.getOptionValue(PASSWORD);
    // if (username != null && password != null) {
    //   String encoded = Base64.getEncoder().encodeToString((username + ':' + password).getBytes(
    //       StandardCharsets.UTF_8));
    //   wc.setHeader("Authorization", "Basic " + encoded);
    // }
    for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(WS_HEADER_PREFIX)) {
        String header = key.substring(WS_HEADER_PREFIX.length());
        System.out.println("Header added to Oozie client: " + header);
        wc.setHeader(header, (String) entry.getValue());
      }
    }
  }

  protected static void setRetryCount(OozieClient wc) {
    String retryCount = System.getProperty(OOZIE_RETRY_COUNT);
    if (retryCount != null && !retryCount.isEmpty()) {
      try {
        int retry = Integer.parseInt(retryCount.trim());
        wc.setRetryCount(retry);
      } catch (Exception ex) {
        System.err.println(
            "Unable to parse the retry settings. May be not an integer [" + retryCount + "]");
        ex.printStackTrace();
      }
    }
  }
}
