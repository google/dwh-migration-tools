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
import java.time.Instant;
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
 * The task collects hosts from Cloudera Manager <a
 * href="https://cldr2-aw-dl-gateway.cldr2-cd.svye-dcxb.a5.cloudera.site/static/apidocs/resource_TimeSeriesResource.html">TimeSeries
 * API</a> The chart is for whole cluster is available on {@code /cmf/home/} and {@code
 * cmf/clusters/{clusterId}/status} pages in Cloudera Manager UI. <b/> The query to chart is written
 * on <a
 * href="https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html">tsquery</a>
 * language.
 */
public class ClouderaClusterCPUChartTask extends AbstractClouderaManagerTask {
  enum TimeSeriesAggregation {
    RAW,
    TEN_MINUTELY,
    HOURLY,
    SIX_HOURLY,
    DAILY,
    WEEKLY,
  }

  private static final Logger LOG = LoggerFactory.getLogger(ClouderaCMFHostsTask.class);
  /*
    SELECT cpu_percent_across_hosts WHERE entityName = "1546336862" AND category = CLUSTER
  */
  private static final String TS_CPU_QUERY_TEMPLATE =
      "SELECT cpu_percent_across_hosts WHERE entityName = \"%s\" AND category = CLUSTER";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final ZonedDateTime from;
  private final TimeSeriesAggregation tsAggregation;

  public ClouderaClusterCPUChartTask() {
    this(Instant.now().atZone(ZoneId.of("UTC")), TimeSeriesAggregation.RAW);
  }

  public ClouderaClusterCPUChartTask(ZonedDateTime from, TimeSeriesAggregation tsAggregation) {
    super(
        String.format(
            "cmf-cluster-cpu-%s-%s.jsonl",
            from.toString().replaceAll("[-:\\[\\]ZUTC.]", "").toLowerCase(),
            tsAggregation.toString().toLowerCase()));
    this.from = from;
    this.tsAggregation = tsAggregation;
  }

  @Override
  protected Void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaClusterDTO> clusters = getClustersFromHandle(handle);

    final String timeSeriesAPIUrl = getTimeSeriesUrl(handle.getApiURI().toString());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String cpuPerClusterQuery = getQueryToFetchCPUTimeSeriesOnCluster(cluster.getId());
        LOG.debug(
            "Execute charts query: [{}] for the cluster: [{}].",
            cpuPerClusterQuery,
            cluster.getName());

        LOG.debug(this.tsAggregation.toString());

        URIBuilder uriBuilder = new URIBuilder(timeSeriesAPIUrl);
        uriBuilder.addParameter("query", cpuPerClusterQuery);
        uriBuilder.addParameter("desiredRollup", this.tsAggregation.toString());
        uriBuilder.addParameter("mustUseDesiredRollup", "true");
        uriBuilder.addParameter("from", getISODateTime(this.from));

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
    List<ClouderaClusterDTO> cpuClusters = new ArrayList<>(clusters);
    cpuClusters.removeIf(
        cluster -> {
          if (cluster.getId() == null) {
            LOG.warn(
                "Cloudera cluster id is null for cluster [{}]. "
                    + "Skip CPU metrics for the cluster.",
                cluster.getName());
            // todo it's might be critical data for TCO calculation and we should fail the dump
            // process. Discuss with product
            return true;
          }
          return false;
        });
    return cpuClusters;
  }

  private String getTimeSeriesUrl(String apiUri) {
    return apiUri + "timeseries";
  }

  private String getQueryToFetchCPUTimeSeriesOnCluster(String clusterId) {
    return String.format(TS_CPU_QUERY_TEMPLATE, clusterId);
  }

  private String getISODateTime(ZonedDateTime datetime) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    ;
    return datetime.format(formatter);
  }
}
