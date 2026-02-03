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
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task collects resource allocation (CPU and RAM) usage per host from Cloudera Manager <a
 * href="https://cldr2-aw-dl-gateway.cldr2-cd.svye-dcxb.a5.cloudera.site/static/apidocs/resource_TimeSeriesResource.html">TimeSeries
 * API</a> The chart for the specific host is available on {@code /cmf/home/} and {@code
 * cmf/hardware/hosts/{hostId}/status} pages in Cloudera Manager UI. <b/> The query to chart is
 * written on <a
 * href="https://docs.cloudera.com/documentation/enterprise/latest/topics/cm_dg_tsquery.html">tsquery</a>
 * language.
 */
public class ClouderaHostResourceAllocationChartTask extends AbstractClouderaTimeSeriesTask {

  private static final Logger logger =
      LoggerFactory.getLogger(ClouderaHostResourceAllocationChartTask.class);

  private static final String TS_QUERY_TEMPLATE =
      "select swap_used, physical_memory_used, physical_memory_total, physical_memory_cached, physical_memory_buffers, cpu_user_rate, cpu_system_rate where category = \"HOST\" AND entityName = \"%s\"";

  public ClouderaHostResourceAllocationChartTask(
      ZonedDateTime startDate,
      ZonedDateTime endDate,
      TimeSeriesAggregation tsAggregation,
      TaskCategory taskCategory) {
    super("host-resource-allocation.jsonl", startDate, endDate, tsAggregation, taskCategory);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaHostDTO> hosts = handle.getHosts();
    if (hosts == null) {
      throw new MetadataDumperUsageException(
          "Cloudera hosts must be initialized before host resource allocation charts dumping.");
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaHostDTO host : handle.getHosts()) {
        String queryPerHost = String.format(TS_QUERY_TEMPLATE, host.getId());
        logger.debug(
            "Execute host resource allocation charts query: [{}] for the host: [{}].",
            queryPerHost,
            host.getName());

        JsonNode chartInJson = requestTimeSeriesChart(handle, queryPerHost);

        writer.write(chartInJson.toString());
        writer.write('\n');
      }
    }
  }
}
