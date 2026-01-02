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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
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
 * The task collects resource allocation metrics for <strong>all</strong> individual service roles
 * present on a given host from the Cloudera Manager <a
 * href="https://cldr2-aw-dl-gateway.cldr2-cd.svye-dcxb.a5.cloudera.site/static/apidocs/resource_TimeSeriesResource.html">TimeSeries
 * API</a>.
 *
 * <p>This class provides a comprehensive, component-by-component breakdown of resource consumption,
 * programmatically fetching the data that powers the stacked resource charts on a host's status
 * page in the Cloudera Manager UI (found under the "Charts" tab for a specific host).
 *
 * <p>Queries are written in the <a
 * href="https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html">tsquery</a>
 * language and are filtered by a specific {@code hostId}.
 *
 * <p>Key metrics collected include:
 *
 * <ul>
 *   <li><b>Memory:</b> {@code mem_rss}
 *   <li><b>CPU:</b> {@code cpu_user_rate} and {@code cpu_system_rate}
 *   <li><b>Disk Throughput:</b> {@code read_bytes_rate} and {@code write_bytes_rate}
 * </ul>
 */
public class ClouderaServiceResourceAllocationChartTask extends AbstractClouderaTimeSeriesTask {

  private static final Logger logger =
      LoggerFactory.getLogger(ClouderaServiceResourceAllocationChartTask.class);

  private static final String SERVICE_RESOURCE_ALLOCATION_QUERY_TEMPLATE =
      "select mem_rss, cpu_user_rate, cpu_system_rate, read_bytes_rate, write_bytes_rate where category = \"ROLE\" AND hostId = \"%s\"";

  public ClouderaServiceResourceAllocationChartTask(
      ZonedDateTime startDate,
      ZonedDateTime endDate,
      TimeSeriesAggregation tsAggregation,
      TaskCategory taskCategory) {
    super("service-resource-allocation.jsonl", startDate, endDate, tsAggregation, taskCategory);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaHostDTO> hosts = getHostsFromHandle(handle);

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaHostDTO host : hosts) {
        String resourceAllocationPerHostQuery =
            String.format(SERVICE_RESOURCE_ALLOCATION_QUERY_TEMPLATE, host.getId());
        logger.debug(
            "Execute service resource allocation charts query: [{}] for the host: [{}].",
            resourceAllocationPerHostQuery,
            host.getName());

        JsonNode chartInJson = requestTimeSeriesChart(handle, resourceAllocationPerHostQuery);

        writer.write(chartInJson.toString());
        writer.write('\n');
      }
    }
  }

  private List<ClouderaHostDTO> getHostsFromHandle(@Nonnull ClouderaManagerHandle handle) {
    List<ClouderaHostDTO> hosts = handle.getHosts();
    if (hosts == null) {
      throw new MetadataDumperUsageException(
          "Cloudera hosts must be initialized before service resource allocation charts dumping.");
    }
    List<ClouderaHostDTO> validHosts = new ArrayList<>();
    for (ClouderaHostDTO host : hosts) {
      if (host.getId() == null) {
        logger.warn(
            "Cloudera host id is null for host [{}]. Skip resource allocation metrics for services belonging to this host.",
            host.getName());
      } else {
        validHosts.add(host);
      }
    }
    return validHosts;
  }
}
