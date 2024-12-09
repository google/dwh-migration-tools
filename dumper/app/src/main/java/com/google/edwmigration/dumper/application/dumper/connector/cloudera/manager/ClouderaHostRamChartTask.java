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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
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

public class ClouderaHostRamChartTask extends AbstractClouderaManagerTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaCMFHostsTask.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final int includedLastDays;
  private final TimeSeriesAggregation tsAggregation;

  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static final String TS_CPU_QUERY_TEMPLATE =
      "select swap_used, physical_memory_used, physical_memory_total, physical_memory_cached, physical_memory_buffers where entityName = \"%s\"";

  public ClouderaHostRamChartTask(
      int includedLastDays, @Nonnull TimeSeriesAggregation tsAggregation) {
    super("cmf-cluster-ram.jsonl");
    this.tsAggregation = tsAggregation;
    this.includedLastDays = includedLastDays;
  }

  @Override
  protected Void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaClusterDTO> clusters = getClustersFromHandle(handle);

    final String timeSeriesAPIUrl = buildTimeSeriesUrl(handle.getApiURI().toString());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaHostDTO host : handle.getHosts()) {
        String cpuPerClusterQuery = buildQueryToFetchCPUTimeSeriesOnCluster(host.getId());
        // LOG.debug(
        //     "Execute charts query: [{}] for the cluster: [{}].",
        //     cpuPerClusterQuery,
        //     cluster.getName());

        URIBuilder uriBuilder = new URIBuilder(timeSeriesAPIUrl);
        uriBuilder.addParameter("query", cpuPerClusterQuery);
        uriBuilder.addParameter("desiredRollup", this.tsAggregation.toString());
        uriBuilder.addParameter("mustUseDesiredRollup", "true");
        uriBuilder.addParameter("from", buildISODateTime(this.includedLastDays));

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

  private String buildQueryToFetchCPUTimeSeriesOnCluster(String clusterId) {
    return String.format(TS_CPU_QUERY_TEMPLATE, clusterId);
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dt =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dt.format(isoDateTimeFormatter);
  }

  private String buildTimeSeriesUrl(String apiUri) {
    return apiUri + "/timeseries";
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
