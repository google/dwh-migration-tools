/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.bigquery;

import com.google.api.gax.paging.Page;
import com.google.auto.service.AutoService;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryAccessor;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.ext.jdk.concurrent.ExecutorManager;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentRecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.BigQueryLogsDumpFormat;
import com.swrve.ratelimitedlogger.RateLimitedLog;
import java.io.Writer;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Google BigQuery.")
@RespectsInput(
    order = 600,
    arg = ConnectorArguments.OPT_DATABASE,
    description = "The list of projects from which to dump, separated by commas.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class BigQueryLogsConnector extends AbstractBigQueryConnector
    implements LogsConnector, BigQueryLogsDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryLogsConnector.class);
  private static final Logger LOG_LIMITED =
      RateLimitedLog.withRateLimit(LOG).maxRate(2).every(Duration.ofSeconds(10)).build();
  private static final boolean DEBUG = false;

  public static class QueryLogsTask extends AbstractBigQueryTask
      implements BigQueryLogsDumpFormat.QueryLogsTask {

    private final ConnectorArguments arguments;
    private final BigQuery.JobListOption jloPageSize =
        BigQuery.JobListOption.pageSize(10000); // Is there a server side limit of 1000?
    private final BigQuery.JobListOption jloState =
        BigQuery.JobListOption.stateFilter(JobStatus.State.DONE);
    private final BigQuery.JobListOption jloAllUsers = BigQuery.JobListOption.allUsers();

    public QueryLogsTask(@Nonnull ConnectorArguments arguments) {
      super(QueryHistoryJson.ZIP_ENTRY_NAME);
      this.arguments = arguments;
    }

    @Override
    protected void run(@Nonnull Writer writer, @Nonnull BigQuery bigQuery) throws Exception {
      // BigQuery.JobListOption allUsers = BigQuery.JobListOption.allUsers(); //XXX: using allUsers
      // causes NPE while getting values.

      List<? extends String> projectIds = arguments.getDatabases();
      if (projectIds.isEmpty()) projectIds = ImmutableList.of(bigQuery.getOptions().getProjectId());

      ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);

      // Currently uses Jobs API to get list of query jobs
      // (https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list)
      // Not sure how efficient it is w.r.t to computing and monetary costs.
      // (There is also audit logs. https://cloud.google.com/bigquery/docs/reference/auditlogs/)
      ExecutorService executor = newExecutorService();
      try (ConcurrentProgressMonitor monitor =
              new ConcurrentRecordProgressMonitor("Writing to " + getTargetPath());
          ExecutorManager manager = new ExecutorManager(executor)) {
        for (String projectId : projectIds) {
          for (ZonedInterval interval : intervals) {
            LOG.info(
                "Retrieving query logs in range start={} to end={}",
                interval.getStart(),
                interval.getEndExclusive());
            BigQuery.JobListOption minCreationTimeMillis =
                BigQuery.JobListOption.minCreationTime(interval.getStart().toEpochSecond() * 1000);
            BigQuery.JobListOption maxCreationTimeMillis =
                BigQuery.JobListOption.maxCreationTime(
                    interval.getEndExclusive().toEpochSecond() * 1000);
            Page<Job> jobPage =
                runWithBackOff(
                    () ->
                        BigQueryAccessor.listJobs(
                            bigQuery,
                            projectId,
                            minCreationTimeMillis,
                            maxCreationTimeMillis,
                            jloAllUsers,
                            jloPageSize,
                            jloState));
            Iterable<Job> jobIterable = new PageIterable<>(jobPage);
            writeJobsDetails(manager, bigQuery, writer, jobIterable, monitor);
          }
        }
      } finally {
        shutdown(executor);
      }
    }

    private void writeJobsDetails(
        @Nonnull ExecutorManager manager,
        @Nonnull BigQuery bigQuery,
        @Nonnull Writer writer,
        @Nonnull Iterable<Job> jobs,
        @Nonnull ConcurrentProgressMonitor monitor)
        throws Exception {
      Iterator<Job> it = jobs.iterator();
      while (it.hasNext()) {
        monitor.count();

        Job job;
        try {
          job = it.next();
        } catch (IllegalArgumentException | NullPointerException e) {
          // See https://github.com/googleapis/google-cloud-java/issues/6499
          LOG_LIMITED.warn(
              "Failed to load job (attempting to continue): " + monitor.getCount() + ": " + e, e);
          continue;
        }

        JobId jobId = job.getJobId();
        if (jobId == null || jobId.getJob() == null) {
          if (DEBUG) LOG_LIMITED.debug("No JobId: Assuming dry run. Skipping.");
          // Mark it as a success.
          continue; // It's a dry-run query.
        }

        // See https://cloud.google.com/bigquery/docs/managing-jobs
        JobStatistics statistics = job.getStatistics();
        if (statistics == null) {
          if (DEBUG)
            LOG_LIMITED.debug(
                "No JobStatistics in {}. Skipping. You need bigquery.jobs.listAll permission or bigquery.admin role to list all jobs.",
                jobId);
          continue;
        }

        Long jobChildCount = statistics.getNumChildJobs();
        if (jobChildCount != null && jobChildCount > 0) {
          if (DEBUG)
            LOG_LIMITED.debug(
                "Listing {} children of {}.{}", jobChildCount, jobId.getProject(), jobId);
          BigQuery.JobListOption jloParent = BigQuery.JobListOption.parentJobId(jobId.getJob());
          manager.execute(
              () -> {
                Page<Job> jobChildPage =
                    runWithBackOff(
                        () ->
                            BigQueryAccessor.listJobs(
                                bigQuery, jobId.getProject(), jloParent, jloPageSize));
                Iterable<Job> jobChildIterable = new PageIterable<>(jobChildPage);
                writeJobsDetails(manager, bigQuery, writer, jobChildIterable, monitor);
                return null;
              });
        }

        JobConfiguration configuration = job.getConfiguration();
        if (configuration == null) {
          if (DEBUG) LOG_LIMITED.debug("No JobConfiguration in job: " + job);
          continue;
        }

        QueryHistoryJson.Job out = new QueryHistoryJson.Job();
        out.project = jobId.getProject();
        out.job = jobId.getJob();
        out.userEmail = job.getUserEmail();

        BigQueryError error = job.getStatus().getError();
        if (error != null) {
          QueryHistoryJson.JobStatus outStatus = new QueryHistoryJson.JobStatus();
          outStatus.message = error.getMessage();
          outStatus.reason = error.getReason();
          out.jobStatus = outStatus;
        }

        JobStatistics jobStatistics = job.getStatistics();
        out.startTime = jobStatistics.getStartTime();
        out.endTime = jobStatistics.getEndTime();

        JobConfiguration.Type type = configuration.getType();
        if (type != null) {
          switch (type) {
            case QUERY:
              QueryJobConfiguration queryJobConfiguration = job.getConfiguration();
              if (BooleanUtils.isTrue(queryJobConfiguration.dryRun())) continue;
              addQueryJob(out, job);
              break;
            case LOAD:
              addLoadJob(out, job);
              break;
            case COPY:
            case EXTRACT:
            default:
              LOG_LIMITED.debug("Ignored job of type " + type);
              continue;
          }

          String metadataText = BigQueryLogsDumpFormat.MAPPER.writeValueAsString(out);
          synchronized (writer) {
            writer.write(metadataText);
            writer.write('\n');
          }
        } else {
          LOG_LIMITED.debug("Ignored job with no type.");
        }
      }
    }

    @CheckForNull
    private static String getEnumName(@CheckForNull Enum<?> value) {
      if (value == null) return null;
      return value.name();
    }

    private static void addQueryJob(@Nonnull QueryHistoryJson.Job out, @Nonnull Job job) {
      QueryHistoryJson.QueryJobConfiguration outConfiguration =
          new QueryHistoryJson.QueryJobConfiguration();

      QueryJobConfiguration queryJobConfiguration = job.getConfiguration();

      final String dataset;
      if (queryJobConfiguration.getDefaultDataset() != null)
        dataset = queryJobConfiguration.getDefaultDataset().getDataset();
      else if (queryJobConfiguration.getDestinationTable() != null)
        dataset = queryJobConfiguration.getDestinationTable().getDataset();
      else dataset = "";
      outConfiguration.defaultDataset = dataset;
      outConfiguration.project = out.project;
      outConfiguration.query = queryJobConfiguration.getQuery();

      DESTINATION:
      {
        outConfiguration.createDisposition =
            getEnumName(queryJobConfiguration.getCreateDisposition());
        outConfiguration.writeDisposition =
            getEnumName(queryJobConfiguration.getWriteDisposition());
        TableId destinationTable = queryJobConfiguration.getDestinationTable();
        if (destinationTable != null) {
          QueryHistoryJson.TableId outTable = new QueryHistoryJson.TableId();
          outTable.project = destinationTable.getProject();
          outTable.dataset = destinationTable.getDataset();
          outTable.table = destinationTable.getTable();
          outConfiguration.destinationTable = outTable;
        }
      }

      STATISTICS:
      {
        QueryHistoryJson.QueryStatistics outStatistics = new QueryHistoryJson.QueryStatistics();
        JobStatistics.QueryStatistics queryStatistics = job.getStatistics();
        outStatistics.billingTier = queryStatistics.getBillingTier();
        outStatistics.cacheHit = queryStatistics.getCacheHit();
        outStatistics.estimatedBytesProcessed = queryStatistics.getEstimatedBytesProcessed();
        outStatistics.totalBytesBilled = queryStatistics.getTotalBytesBilled();
        outStatistics.totalBytesProcessed = queryStatistics.getTotalBytesProcessed();
        outStatistics.totalSlotMilliseconds = queryStatistics.getTotalSlotMs();
        outStatistics.dmlAffectedRowCount = queryStatistics.getNumDmlAffectedRows();
        outStatistics.totalPartitionsProcessed = queryStatistics.getTotalPartitionsProcessed();
        outConfiguration.statistics = outStatistics;
      }

      out.labels = queryJobConfiguration.getLabels();
      out.queryJobConfiguration = outConfiguration;
    }

    private static void addLoadJob(@Nonnull QueryHistoryJson.Job out, @Nonnull Job job) {
      QueryHistoryJson.LoadJobConfiguration outConfiguration =
          new QueryHistoryJson.LoadJobConfiguration();
      LoadJobConfiguration loadConfiguration = job.getConfiguration();

      outConfiguration.sourceUris = loadConfiguration.getSourceUris();
      outConfiguration.sourceFormat = loadConfiguration.getFormat();

      DESTINATION:
      {
        outConfiguration.createDisposition = getEnumName(loadConfiguration.getCreateDisposition());
        outConfiguration.writeDisposition = getEnumName(loadConfiguration.getWriteDisposition());
        TableId destinationTable = loadConfiguration.getDestinationTable();
        if (destinationTable != null) {
          QueryHistoryJson.TableId outTable = new QueryHistoryJson.TableId();
          outTable.project = destinationTable.getProject();
          outTable.dataset = destinationTable.getDataset();
          outTable.table = destinationTable.getTable();
          outConfiguration.destinationTable = outTable;
        }
      }

      STATISTICS:
      {
        QueryHistoryJson.LoadStatistics outStatistics = new QueryHistoryJson.LoadStatistics();
        JobStatistics.LoadStatistics loadStatistics = job.getStatistics();
        outStatistics.badRecords = loadStatistics.getBadRecords();
        outStatistics.inputBytes = loadStatistics.getInputBytes();
        outStatistics.inputFiles = loadStatistics.getInputFiles();
        outStatistics.outputBytes = loadStatistics.getOutputBytes();
        outStatistics.outputRows = loadStatistics.getOutputRows();
        outConfiguration.statistics = outStatistics;
      }

      out.labels = loadConfiguration.getLabels();
      out.loadJobConfiguration = outConfiguration;
    }

    @Override
    protected String toCallDescription() {
      return "BigQuery.Jobs().list()*";
    }
  }

  public BigQueryLogsConnector() {
    super("bigquery-logs");
  }

  @Override
  // TODO: Use ZonedIntervalIterable.forConnectorArguments() to generate N tasks here, each for a
  // window, rather than just using one task with arguments.
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));
    out.add(new QueryLogsTask(arguments));
  }
}
