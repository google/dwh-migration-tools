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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.net.URI;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nonnull;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

abstract class AbstractClouderaTimeSeriesTask extends AbstractClouderaManagerTask {
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private final ZonedDateTime startDate;
  private final ZonedDateTime endDate;
  private final TimeSeriesAggregation tsAggregation;
  private final TaskCategory taskCategory;

  public AbstractClouderaTimeSeriesTask(
      @Nonnull String targetPath,
      @Nonnull ZonedDateTime startDate,
      @Nonnull ZonedDateTime endDate,
      @Nonnull TimeSeriesAggregation tsAggregation,
      @Nonnull TaskCategory taskCategory) {
    super(targetPath);
    Preconditions.checkNotNull(targetPath, "Target path must be not null.");
    Preconditions.checkState(!targetPath.isEmpty(), "Target file path must be not empty.");
    Preconditions.checkNotNull(tsAggregation, "TimeSeriesAggregation must be not null.");
    Preconditions.checkNotNull(startDate, "Start date must be not null.");
    Preconditions.checkNotNull(endDate, "End date must be not null.");
    Preconditions.checkState(startDate.isBefore(endDate), "Start Date has to be before End Date.");

    this.startDate = startDate;
    this.endDate = endDate;
    this.tsAggregation = tsAggregation;
    this.taskCategory = taskCategory;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return taskCategory;
  }

  protected JsonNode requestTimeSeriesChart(ClouderaManagerHandle handle, String query)
      throws Exception {
    String timeSeriesUrl = handle.getApiURI().toString() + "/timeseries";

    URIBuilder uriBuilder = new URIBuilder(timeSeriesUrl);
    uriBuilder.addParameter("query", query);
    uriBuilder.addParameter("desiredRollup", tsAggregation.toString());
    uriBuilder.addParameter("mustUseDesiredRollup", "true");
    uriBuilder.addParameter("from", startDate.format(isoDateTimeFormatter));
    uriBuilder.addParameter("to", endDate.format(isoDateTimeFormatter));
    URI tsURI = uriBuilder.build();

    CloseableHttpClient httpClient = handle.getHttpClient();
    JsonNode chartInJson;
    try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(tsURI))) {
      int statusCode = chart.getStatusLine().getStatusCode();
      if (!isStatusCodeOK(statusCode)) {
        throw new RuntimeException(
            String.format(
                "Cloudera Error: Response status code is %d but 2xx is expected.", statusCode));
      }
      chartInJson = readJsonTree(chart.getEntity().getContent());
    }
    return chartInJson;
  }

  enum TimeSeriesAggregation {
    RAW,
    TEN_MINUTELY,
    HOURLY,
    SIX_HOURLY,
    DAILY,
    WEEKLY,
  }
}
