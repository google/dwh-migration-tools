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
import java.time.LocalDateTime;
import java.time.ZoneId;
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
  private final int includedLastDays;
  private final TimeSeriesAggregation tsAggregation;
  private final TaskCategory taskCategory;

  public AbstractClouderaTimeSeriesTask(
      String targetPath,
      int includedLastDays,
      TimeSeriesAggregation tsAggregation,
      TaskCategory taskCategory) {
    super(targetPath);
    Preconditions.checkNotNull(tsAggregation, "TimeSeriesAggregation must not be null.");
    Preconditions.checkArgument(
        includedLastDays >= 1,
        "The chart has to include at least one day. Received " + includedLastDays + " days.");
    Preconditions.checkNotNull(taskCategory, "TaskCategory must not be null.");

    this.includedLastDays = includedLastDays;
    this.tsAggregation = tsAggregation;
    this.taskCategory = taskCategory;
  }

  @Nonnull
  @Override
  public final TaskCategory getCategory() {
    return taskCategory;
  }

  protected JsonNode requestTimeSeriesChart(ClouderaManagerHandle handle, String query)
      throws Exception {
    String timeSeriesUrl = handle.getApiURI().toString() + "/timeseries";
    String fromDate = buildISODateTime(includedLastDays);

    URIBuilder uriBuilder = new URIBuilder(timeSeriesUrl);
    uriBuilder.addParameter("query", query);
    uriBuilder.addParameter("desiredRollup", tsAggregation.toString());
    uriBuilder.addParameter("mustUseDesiredRollup", "true");
    uriBuilder.addParameter("from", fromDate);
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

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dateTime.format(isoDateTimeFormatter);
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
