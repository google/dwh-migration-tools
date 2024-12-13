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
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaHostDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClouderaHostRAMChartTask extends AbstractClouderaTimeSeriesTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaCMFHostsTask.class);

  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static final String TS_RAM_QUERY_TEMPLATE =
      "select swap_used, physical_memory_used, physical_memory_total, physical_memory_cached, physical_memory_buffers where entityName = \"%s\"";

  public ClouderaHostRAMChartTask(int includedLastDays, TimeSeriesAggregation tsAggregation) {
    super(buildOutputFileName(includedLastDays));
    Preconditions.checkNotNull(tsAggregation, "TimeSeriesAggregation has not to be a null.");
    Preconditions.checkArgument(
        includedLastDays >= 1,
        "The chart has to include at least one day. Received " + includedLastDays + " days.");
    this.includedLastDays = includedLastDays;
    this.tsAggregation = tsAggregation;
  }

  @Override
  protected Void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaHostDTO> hosts = handle.getHosts();
    if (hosts == null) {
      throw new MetadataDumperUsageException(
          "Cloudera hosts must be initialized before RAM charts dumping.");
    }

    String includedDaysInIsoFormat = buildISODateTime(includedLastDays);
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaHostDTO host : handle.getHosts()) {
        String ramPerHostQuery = buildTimeSeriesQuery(host.getId());
        LOG.debug(
            "Execute RAM charts query: [{}] for the host: [{}].", ramPerHostQuery, host.getName());

        JsonNode chartInJson =
            requestTimeSeriesChart(handle, ramPerHostQuery, includedDaysInIsoFormat);
        writer.write(chartInJson.toString());
        writer.write('\n');
      }
    }
    return null;
  }

  static String buildOutputFileName(int includedLastDays) {
    return String.format("host-ram-%sd.jsonl", includedLastDays);
  }

  private String buildTimeSeriesQuery(String hostId) {
    return String.format(TS_RAM_QUERY_TEMPLATE, hostId);
  }
}
