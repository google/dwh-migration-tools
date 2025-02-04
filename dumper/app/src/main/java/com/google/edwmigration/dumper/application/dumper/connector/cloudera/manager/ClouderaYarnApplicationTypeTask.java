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
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationListDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationTypeDTO;
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

public class ClouderaYarnApplicationTypeTask extends AbstractClouderaManagerTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaYarnApplicationsTask.class);
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private final ImmutableList<String> predefinedAppTypes = ImmutableList.of("MAPREDUCE", "SPARK");
  private final int includedLastDays;

  public ClouderaYarnApplicationTypeTask(int days) {
    super(String.format("yarn-application-types-%dd.jsonl", days));
    this.includedLastDays = days;
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    Preconditions.checkNotNull(
        clusters, "Clusters must be initialized before fetching YARN applications.");

    List<ApplicationTypeToYarnApplication> totalYARNApplications = new ArrayList<>();
    PaginatedYarnApplicationsLoader appLoader =
        new PaginatedYarnApplicationsLoader(handle.getApiURI().toString(), handle.getHttpClient());
    for (ClouderaClusterDTO cluster : handle.getClusters()) {
      String clusterName = cluster.getName();
      List<String> yarnAppTypes = fetchYARNApplicationTypes(handle, clusterName);
      for (String yarnAppType : yarnAppTypes) {
        LOG.info("Dump YARN applications with {} type from {} cluster", yarnAppType, clusterName);
        List<ApplicationTypeToYarnApplication> yarnApplications =
            appLoader.load(clusterName, yarnAppType);
        totalYARNApplications.addAll(yarnApplications);
        LOG.info(
            "Dumped {} YARN applications with {} type from {} cluster",
            yarnApplications.size(),
            yarnAppType,
            clusterName);
      }
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

  private List<String> fetchYARNApplicationTypes(ClouderaManagerHandle handle, String clusterName) {
    List<String> yarnApplicationTypes = new ArrayList<>(predefinedAppTypes);
    String yarnAppTypesUrl =
        handle.getApiURI().toString() + "clusters/" + clusterName + "/serviceTypes";
    CloseableHttpClient httpClient = handle.getHttpClient();
    JsonNode yarnAppTypesJson;
    ApiYARNApplicationTypeDTO yarnAppTypesFromClouderaAPI;
    try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(yarnAppTypesUrl))) {
      yarnAppTypesJson = readJsonTree(chart.getEntity().getContent());
      yarnAppTypesFromClouderaAPI =
          parseJsonStringToObject(yarnAppTypesJson.toString(), ApiYARNApplicationTypeDTO.class);
      yarnApplicationTypes.addAll(yarnAppTypesFromClouderaAPI.getItems());
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    return yarnApplicationTypes;
  }

  private class PaginatedYarnApplicationsLoader {
    private final String host;
    private final CloseableHttpClient httpClient;
    private final int limit;
    private int offset;

    public PaginatedYarnApplicationsLoader(String host, CloseableHttpClient httpClient) {
      this.host = host;
      this.httpClient = httpClient;
      this.limit = 100;
      this.offset = 0;
    }

    public List<ApplicationTypeToYarnApplication> load(String clusterName, String appType) {
      List<ApplicationTypeToYarnApplication> yarnApplications = new LinkedList<>();
      offset = 0;
      boolean nextLoad = true;

      while (nextLoad) {
        URI yarnAppsURI = buildYARNApplicationURI(clusterName, appType);
        List<ApiYARNApplicationDTO> newLoad = load(yarnAppsURI);
        for (ApiYARNApplicationDTO app : newLoad) {
          yarnApplications.add(
              new ApplicationTypeToYarnApplication(app.getApplicationId(), appType));
        }
        nextLoad = (!newLoad.isEmpty());
        offset += limit;
      }
      return yarnApplications;
    }

    private List<ApiYARNApplicationDTO> load(URI yarnAppURI) {
      try (CloseableHttpResponse resp = httpClient.execute(new HttpGet(yarnAppURI))) {
        JsonNode yarnApplicationsRespJson = readJsonTree(resp.getEntity().getContent());
        ApiYARNApplicationListDTO yarnAppListDto =
            parseJsonStringToObject(
                yarnApplicationsRespJson.toString(), ApiYARNApplicationListDTO.class);
        return yarnAppListDto.getApplications();
      } catch (IOException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }

    private URI buildYARNApplicationURI(String clusterName, String appType) {
      String yarnApplicationsUrl =
          host + "clusters/" + clusterName + "/services/yarn/yarnApplications";
      String fromDate = buildISODateTime(includedLastDays);
      URI yarnApplicationsURI;
      try {
        URIBuilder uriBuilder = new URIBuilder(yarnApplicationsUrl);
        uriBuilder.addParameter("limit", String.valueOf(limit));
        uriBuilder.addParameter("offset", String.valueOf(offset));
        uriBuilder.addParameter("from", fromDate);
        uriBuilder.addParameter("filter", String.format("applicationType=%s", appType));
        yarnApplicationsURI = uriBuilder.build();
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
      return yarnApplicationsURI;
    }
  }
}

class ApplicationTypeToYarnApplication {
  private final String applicationId;
  private final String applicationType;

  public ApplicationTypeToYarnApplication(String applicationId, String applicationType) {
    this.applicationId = applicationId;
    this.applicationType = applicationType;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getApplicationType() {
    return applicationType;
  }
}
