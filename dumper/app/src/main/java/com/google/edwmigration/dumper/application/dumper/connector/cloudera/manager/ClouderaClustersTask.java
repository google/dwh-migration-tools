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
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiClusterListDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClouderaClustersTask extends AbstractClouderaManagerTask {

  private static final Logger LOG = LoggerFactory.getLogger(ClouderaClustersTask.class);

  public ClouderaClustersTask() {
    super("clusters.json");
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    CloseableHttpClient httpClient = handle.getHttpClient();

    ApiClusterListDTO clusterList;

    if (context.getArguments().getCluster() != null) {
      final String clusterName = context.getArguments().getCluster();
      try (CloseableHttpResponse clusterResponse =
          httpClient.execute(new HttpGet(handle.getApiURI() + "/clusters/" + clusterName))) {

        ApiClusterDTO cluster =
            parseJsonStringToObject(
                EntityUtils.toString(clusterResponse.getEntity()), ApiClusterDTO.class);

        clusterList = new ApiClusterListDTO();
        clusterList.setClusters(ImmutableList.of(cluster));
      }
    } else {
      LOG.info("'--cluster' argument wasn't provided. Collect all available clusters.");

      try (CloseableHttpResponse clustersResponse =
          httpClient.execute(new HttpGet(handle.getApiURI() + "/clusters"))) {
        String clustersJson = EntityUtils.toString(clustersResponse.getEntity());
        clusterList = parseJsonStringToObject(clustersJson, ApiClusterListDTO.class);
      }
    }

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      writer.write(parseObjectToJsonString(clusterList));
    }

    List<ClouderaClusterDTO> clusters = new ArrayList<>();
    for (ApiClusterDTO item : clusterList.getClusters()) {
      String clusterId = requestClusterIdByName(httpClient, handle.getBaseURI(), item.getName());
      clusters.add(ClouderaClusterDTO.create(clusterId, item.getName()));
    }
    LOG.info(
        "Dump metadata for clusters: {}",
        clusters.stream().map(ClouderaClusterDTO::getName).collect(Collectors.toList()));

    handle.initClusters(clusters);
  }

  private String requestClusterIdByName(
      CloseableHttpClient httpClient, URI baseUri, String clusterName) throws Exception {
    String requestUrl = baseUri + "/cmf/clusters/" + clusterName + "/status.json";

    try (CloseableHttpResponse clusterStatus = httpClient.execute(new HttpGet(requestUrl))) {
      if (HttpStatus.SC_OK != clusterStatus.getStatusLine().getStatusCode()) {
        String responseBody = EntityUtils.toString(clusterStatus.getEntity());
        LOG.warn(
            "Can't receive cluster [{}] status by url [{}]. Response status is [{}] and body: {}",
            clusterName,
            requestUrl,
            clusterStatus.getStatusLine().getStatusCode(),
            responseBody);

        return null;
      } else {
        JsonNode jsonNode = readJsonTree(clusterStatus.getEntity().getContent());
        // https://www.rfc-editor.org/rfc/rfc6901
        return jsonNode.at("/clusterModel/id").asText();
      }
    }
  }
}
