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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationListDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The task dumps YARN applications from Cloudera Manager API */
public class ClouderaYarnApplicationsTask extends AbstractClouderaManagerTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaYarnApplicationsTask.class);
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private final int includedLastDays;

  public ClouderaYarnApplicationsTask(int days) {
    super(String.format("yarn-applications-%dd.jsonl", days));
    this.includedLastDays = days;
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    Preconditions.checkNotNull(
        clusters, "Clusters must be initialized before fetching YARN applications.");

    List<ApiYARNApplicationDTO> totalYARNApplications = new ArrayList<>();
    YarnApplicationsLoader appLoader =
        new YarnApplicationsLoader(handle.getApiURI().toString(), handle.getHttpClient());

    for (ClouderaClusterDTO cluster : handle.getClusters()) {
      String clusterName = cluster.getName();
      LOG.info("Dump YARN applications from {} cluster", clusterName);
      List<ApiYARNApplicationDTO> yarnApplications = appLoader.load(clusterName);
      totalYARNApplications.addAll(yarnApplications);
      LOG.info("Dumped {} YARN applications from {} cluster", yarnApplications.size(), clusterName);
    }

    String yarnAppsJson = parseObjectToJsonString(totalYARNApplications);
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      writer.write(yarnAppsJson);
    }
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dateTime.format(isoDateTimeFormatter);
  }

  private class YarnApplicationsLoader {
    private final String host;
    private final CloseableHttpClient httpClient;

    public YarnApplicationsLoader(String host, CloseableHttpClient httpClient) {
      this.host = host;
      this.httpClient = httpClient;
    }

    public List<ApiYARNApplicationDTO> load(String clusterName) {
      List<ApiYARNApplicationDTO> yarnApplications = new LinkedList<>();
      final int limit = 100;
      int offset = 0;
      boolean nextLoad = true;

      while (nextLoad) {
        List<ApiYARNApplicationDTO> newLoad = load(clusterName, limit, offset);
        yarnApplications.addAll(newLoad);
        nextLoad = (!newLoad.isEmpty());
        offset += limit;
      }

      return yarnApplications;
    }

    private List<ApiYARNApplicationDTO> load(String clusterName, int limit, int offset) {
      String yarnApplicationsUrl =
          host + "clusters/" + clusterName + "/services/yarn/yarnApplications";
      String fromDate = buildISODateTime(includedLastDays);
      URI yarnApplicationsURI;
      try {
        URIBuilder uriBuilder = new URIBuilder(yarnApplicationsUrl);
        uriBuilder.addParameter("limit", String.valueOf(limit));
        uriBuilder.addParameter("offset", String.valueOf(offset));
        uriBuilder.addParameter("from", fromDate);
        yarnApplicationsURI = uriBuilder.build();
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }

      try (CloseableHttpResponse resp = httpClient.execute(new HttpGet(yarnApplicationsURI))) {
        JsonNode yarnApplicationsRespJson = readJsonTree(resp.getEntity().getContent());
        ApiYARNApplicationListDTO yarnAppListDto =
            parseJsonStringToObject(
                yarnApplicationsRespJson.toString(), ApiYARNApplicationListDTO.class);
        List<ApiYARNApplicationDTO> yarnApps = yarnAppListDto.getApplications();
        for (ApiYARNApplicationDTO app : yarnApps) {
          app.setClusterName(clusterName);
        }
        return yarnApps;
      } catch (IOException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }
  }
}
