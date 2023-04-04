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
package com.google.cloud.bigquery;

import static com.google.cloud.BaseService.EXCEPTION_HANDLER;
import static com.google.cloud.RetryHelper.runWithRetries;
import static com.google.cloud.bigquery.BigQueryImpl.optionMap;

import com.google.api.gax.paging.Page;
import com.google.cloud.PageImpl;
import com.google.cloud.PageImpl.NextPageFetcher;
import com.google.cloud.RetryHelper;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.BigQuery.JobListOption;
import com.google.cloud.bigquery.JobStatistics.CopyStatistics;
import com.google.cloud.bigquery.JobStatistics.ExtractStatistics;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.JobStatistics.QueryStatistics;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Things which are rude.
 *
 * @author shevek
 */
public class BigQueryAccessor {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryAccessor.class);

  private static class JobPageFetcher implements NextPageFetcher<Job> {

    private static final long serialVersionUID = 8536533282558245472L;
    private final Map<BigQueryRpc.Option, ?> requestOptions;
    private final BigQueryOptions serviceOptions;
    private final String projectId;

    JobPageFetcher(
        BigQueryOptions serviceOptions,
        String projectId,
        String cursor,
        Map<BigQueryRpc.Option, ?> optionMap) {
      this.requestOptions =
          PageImpl.nextRequestOptions(BigQueryRpc.Option.PAGE_TOKEN, cursor, optionMap);
      this.serviceOptions = serviceOptions;
      this.projectId = projectId;
    }

    @Override
    public Page<Job> getNextPage() {
      return listJobs(serviceOptions, projectId, requestOptions);
    }
  }

  @CheckForNull
  @SuppressWarnings("unchecked")
  public static <T extends JobStatistics> T JobStatistics_fromPb(
      com.google.api.services.bigquery.model.Job jobPb) {
    com.google.api.services.bigquery.model.JobConfiguration jobConfigPb = jobPb.getConfiguration();
    // This is the critical null-check to work aroud
    // https://github.com/googleapis/java-bigquery/issues/186
    if (jobConfigPb == null) return null;
    com.google.api.services.bigquery.model.JobStatistics statisticPb = jobPb.getStatistics();
    if (jobConfigPb.getLoad() != null) {
      return (T) LoadStatistics.fromPb(statisticPb);
    } else if (jobConfigPb.getExtract() != null) {
      return (T) ExtractStatistics.fromPb(statisticPb);
    } else if (jobConfigPb.getQuery() != null) {
      return (T) QueryStatistics.fromPb(statisticPb);
    } else if (jobConfigPb.getCopy() != null) {
      return (T) CopyStatistics.fromPb(statisticPb);
    } else {
      throw new IllegalArgumentException("unknown job configuration: " + jobConfigPb);
    }
  }

  @Nonnull
  public static JobInfo.BuilderImpl JobInfo_BuilderImpl_new(
      com.google.api.services.bigquery.model.Job jobPb) {
    JobInfo.BuilderImpl out = new JobInfo.BuilderImpl();
    out.setEtag(jobPb.getEtag());
    out.setGeneratedId(jobPb.getId());
    if (jobPb.getJobReference() != null) {
      out.setJobId(JobId.fromPb(jobPb.getJobReference()));
    }
    out.setSelfLink(jobPb.getSelfLink());
    if (jobPb.getStatus() != null) {
      out.setStatus(JobStatus.fromPb(jobPb.getStatus()));
    }
    if (jobPb.getStatistics() != null) {
      JobStatistics statistics = JobStatistics_fromPb(jobPb);
      if (statistics != null) {
        out.setStatistics(statistics);
      }
    }
    out.setUserEmail(jobPb.getUserEmail());
    if (jobPb.getConfiguration() != null) {
      out.setConfiguration(JobConfiguration.fromPb(jobPb.getConfiguration()));
    }
    return out;
  }

  @Nonnull
  public static Page<Job> listJobs(
      @Nonnull BigQueryOptions serviceOptions,
      @Nonnull String projectId,
      @Nonnull Map<BigQueryRpc.Option, ?> optionsMap)
      throws BigQueryException, RetryHelper.RetryHelperException {
    // LOG.debug("List jobs: {}.{}", projectId, optionsMap);
    Tuple<String, Iterable<com.google.api.services.bigquery.model.Job>> result =
        runWithRetries(
            new Callable<Tuple<String, Iterable<com.google.api.services.bigquery.model.Job>>>() {
              @Override
              public Tuple<String, Iterable<com.google.api.services.bigquery.model.Job>> call() {
                BigQueryRpc rpc = (BigQueryRpc) serviceOptions.getRpc();
                return rpc.listJobs(projectId, optionsMap);
              }
            },
            serviceOptions.getRetrySettings(),
            EXCEPTION_HANDLER,
            serviceOptions.getClock());
    String cursor = result.x();
    Iterable<Job> jobs =
        Iterables.transform(
            result.y(),
            new Function<com.google.api.services.bigquery.model.Job, Job>() {
              @Override
              public Job apply(com.google.api.services.bigquery.model.Job job) {
                // Job.Builder builder = Job.newBuilder(serviceOptions.getService(),
                // JobConfiguration.fr
                // JobStatistics.fromPb(job);
                return new Job(serviceOptions.getService(), JobInfo_BuilderImpl_new(job));
              }
            });
    return new PageImpl<>(
        new JobPageFetcher(serviceOptions, projectId, cursor, optionsMap), cursor, jobs);
  }

  @Nonnull
  public static Page<Job> listJobs(BigQuery bigquery, String projectId, JobListOption... options)
      throws BigQueryException, RetryHelper.RetryHelperException {
    BigQueryOptions serviceOptions = bigquery.getOptions();
    return listJobs(serviceOptions, projectId, optionMap(options));
  }
}
