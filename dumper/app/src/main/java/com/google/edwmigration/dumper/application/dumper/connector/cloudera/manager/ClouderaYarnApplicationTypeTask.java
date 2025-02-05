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
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationTypeDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClouderaYarnApplicationTypeTask extends AbstractClouderaYarnApplicationTask {
  private static final Logger LOG = LoggerFactory.getLogger(ClouderaYarnApplicationsTask.class);

  private final ImmutableList<String> predefinedAppTypes = ImmutableList.of("MAPREDUCE", "SPARK");

  public ClouderaYarnApplicationTypeTask(int days) {
    super("yarn-application-types", days);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    Preconditions.checkNotNull(
        clusters, "Clusters must be initialized before fetching YARN applications.");

    PaginatedClouderaYarnApplicationsLoader appLoader =
        new PaginatedClouderaYarnApplicationsLoader(handle);
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        String clusterName = cluster.getName();
        List<String> yarnAppTypes = fetchYARNApplicationTypes(handle, clusterName);
        for (String yarnAppType : yarnAppTypes) {
          LOG.info("Dump YARN applications with {} type from {} cluster", yarnAppType, clusterName);
          int loadedAppsCnt =
              appLoader.load(
                  clusterName,
                  yarnAppType,
                  yarnAppsPage -> {
                    List<ApplicationTypeToYarnApplication> yarnTypeMaps = new ArrayList<>();
                    for (ApiYARNApplicationDTO yarnApp : yarnAppsPage) {
                      yarnTypeMaps.add(
                          new ApplicationTypeToYarnApplication(
                              yarnApp.getApplicationId(), yarnAppType));
                    }
                    try {
                      String yarnTypeMapsInJson = parseObjectToJsonString(yarnTypeMaps);
                      writer.write(yarnTypeMapsInJson);
                      writer.write('\n');
                    } catch (IOException ex) {
                      throw new RuntimeException("Error: Can't dump YARN application types", ex);
                    }
                  });
          LOG.info(
              "Dumped {} YARN applications with {} type from {} cluster",
              loadedAppsCnt,
              yarnAppType,
              clusterName);
        }
      }
    }
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
