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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task collects hosts from Cloudera Manager {@code /cmf/} urls. These API contains
 * well-structured data but is not well documented.
 */
public class ClouderaCMFHostsTask extends AbstractClouderaManagerTask {

  private static final Logger logger = LoggerFactory.getLogger(ClouderaCMFHostsTask.class);

  public ClouderaCMFHostsTask() {
    super("cmf-hosts.jsonl");
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    if (clusters == null) {
      throw new IllegalStateException(
          "Cloudera clusters must be initialized before hosts dumping.");
    }

    final URI baseURI = handle.getBaseURI();
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        if (cluster.getId() == null) {
          logger.warn(
              "Cloudera cluster id is null for cluster [{}]. "
                  + "Skip dumping hosts overview for the cluster.",
              cluster.getName());
          continue;
        }

        String hostPerClusterUrl =
            baseURI + "/cmf/hardware/hosts/hostsOverview.json?clusterId=" + cluster.getId();

        JsonNode hostsJson;
        try (CloseableHttpResponse hostsResponse =
            httpClient.execute(new HttpGet(hostPerClusterUrl))) {
          try {
            hostsJson = readJsonTree(hostsResponse.getEntity().getContent());
          } catch (JsonParseException ex) {
            logger.warn(
                "Could not parse json from cloudera hosts response: " + ex.getMessage(), ex);
            continue;
          }
        }
        String stringifiedHosts = hostsJson.toString();
        writer.write(stringifiedHosts);
        writer.write('\n');
      }
    }
  }
}
