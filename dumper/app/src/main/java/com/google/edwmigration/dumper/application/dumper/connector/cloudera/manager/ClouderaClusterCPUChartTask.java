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
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
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
public class ClouderaClusterCPUChartTask extends AbstractClouderaTimeSeriesTask {
  private static final Logger logger = LoggerFactory.getLogger(ClouderaClusterCPUChartTask.class);
  private static final String TS_CPU_QUERY_TEMPLATE =
      "SELECT cpu_percent_across_hosts WHERE entityName = \"%s\" AND category = CLUSTER";

  public ClouderaClusterCPUChartTask(
      ZonedDateTime startDate,
      ZonedDateTime endDate,
      TimeSeriesAggregation tsAggregation,
      TaskCategory taskCategory) {
    super("cluster-cpu.jsonl", startDate, endDate, tsAggregation, taskCategory);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = getClustersFromHandle(handle);

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String cpuPerClusterQuery = String.format(TS_CPU_QUERY_TEMPLATE, cluster.getId());
        logger.debug(
            "Execute charts query: [{}] for the cluster: [{}].",
            cpuPerClusterQuery,
            cluster.getName());

        JsonNode chartInJson = requestTimeSeriesChart(handle, cpuPerClusterQuery);
        writer.write(chartInJson.toString());
        writer.write('\n');
      }
    }
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
        logger.warn(
            "Cloudera cluster id is null for cluster [{}]. Skip CPU metrics for the cluster.",
            cluster.getName());
      } else {
        cpuClusters.add(cluster);
      }
    }
    return cpuClusters;
  }
}
