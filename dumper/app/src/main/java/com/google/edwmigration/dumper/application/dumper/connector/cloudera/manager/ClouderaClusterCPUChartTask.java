/*
 * Copyright 2022-2024 Google LLC
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task collects CPU usage per cluster from Cloudera Manager <a
 * href="https://cldr2-aw-dl-gateway.cldr2-cd.svye-dcxb.a5.cloudera.site/static/apidocs/resource_TimeSeriesResource.html">TimeSeries
 * API</a> The chart is for whole cluster is available on {@code /cmf/home/} and {@code
 * cmf/clusters/{clusterId}/status} pages in Cloudera Manager UI. <b/> The query to chart is written
 * on <a
 * href="https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html">tsquery</a>
 * language.
 */
public class ClouderaClusterCPUChartTask extends AbstractClouderaManagerTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaCMFHostsTask.class);
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private final ObjectMapper objectMapper = new ObjectMapper();

  private final ClouderaTimeSeriesQueryBuilder tsQueryBuilder;

  public ClouderaClusterCPUChartTask(ClouderaTimeSeriesQueryBuilder tsQueryBuilder) {
    super(buildOutputFileName(tsQueryBuilder.getIncludedLastDays()));
    this.tsQueryBuilder = tsQueryBuilder;
  }

  @Override
  protected Void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaClusterDTO> clusters = getClustersFromHandle(handle);

    final String timeSeriesAPIUrl = buildTimeSeriesUrl(handle.getApiURI().toString());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String cpuPerClusterQuery = tsQueryBuilder.getQuery(cluster.getId());
        LOG.debug(
            "Execute charts query: [{}] for the cluster: [{}].",
            cpuPerClusterQuery,
            cluster.getName());

        URIBuilder uriBuilder = new URIBuilder(timeSeriesAPIUrl);
        uriBuilder.addParameter("query", cpuPerClusterQuery);
        uriBuilder.addParameter("desiredRollup", tsQueryBuilder.getTsAggregation().toString());
        uriBuilder.addParameter("mustUseDesiredRollup", "true");
        uriBuilder.addParameter("from", buildISODateTime(tsQueryBuilder.getIncludedLastDays()));

        try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(uriBuilder.build()))) {
          JsonNode jsonNode = objectMapper.readTree(chart.getEntity().getContent());
          writer.write(jsonNode.toString());
          writer.write('\n');
        }
      }
    }

    return null;
  }

  private List<ClouderaClusterDTO> getClustersFromHandle(@Nonnull ClouderaManagerHandle handle) {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    if (clusters == null) {
      throw new MetadataDumperUsageException(
          "Cloudera clusters must be initialized before CPU charts dumping.");
    }
    List<ClouderaClusterDTO> cpuClusters = new ArrayList<>();
    for (ClouderaClusterDTO cluster : clusters) {
      if (cluster.getId() == null) {
        LOG.warn(
            "Cloudera cluster id is null for cluster [{}]. Skip CPU metrics for the cluster.",
            cluster.getName());
      } else {
        cpuClusters.add(cluster);
      }
    }
    return cpuClusters;
  }

  private String buildTimeSeriesUrl(String apiUri) {
    return apiUri + "/timeseries";
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dt =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dt.format(isoDateTimeFormatter);
  }

  private static String buildOutputFileName(int includedLastDays) {
    return String.format("cluster-cpu-%sd.jsonl", includedLastDays);
  }
}
