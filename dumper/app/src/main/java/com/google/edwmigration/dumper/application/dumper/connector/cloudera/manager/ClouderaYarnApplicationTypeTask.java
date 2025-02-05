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
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
        final String clusterName = cluster.getName();
        Set<String> yarnAppTypes = new HashSet<>(fetchYARNApplicationTypes(handle, clusterName));
        yarnAppTypes.addAll(predefinedAppTypes);
        for (String yarnAppType : yarnAppTypes) {
          LOG.info("Dump YARN applications with {} type from {} cluster", yarnAppType, clusterName);
          int loadedAppsCnt =
              appLoader.load(
                  clusterName,
                  yarnAppType,
                  yarnAppsPage -> writeYarnAppTypes(writer, yarnAppsPage, yarnAppType));
          LOG.info(
              "Dumped {} YARN applications with {} type from {} cluster",
              loadedAppsCnt,
              yarnAppType,
              clusterName);
        }
      }
    }
  }

  private void writeYarnAppTypes(
      Writer writer, List<ApiYARNApplicationDTO> yarnApps, String appType) {
    List<ApplicationTypeToYarnApplication> yarnTypeMaps = new ArrayList<>();
    for (ApiYARNApplicationDTO yarnApp : yarnApps) {
      yarnTypeMaps.add(new ApplicationTypeToYarnApplication(yarnApp.getApplicationId(), appType));
    }
    try {
      String yarnTypeMapsInJson = parseObjectToJsonString(yarnTypeMaps);
      writer.write(yarnTypeMapsInJson);
      writer.write('\n');
    } catch (IOException ex) {
      throw new RuntimeException("Error: Can't dump YARN application types", ex);
    }
  }

  private List<String> fetchYARNApplicationTypes(ClouderaManagerHandle handle, String clusterName) {
    String yarnAppTypesUrl =
        handle.getApiURI().toString() + "clusters/" + clusterName + "/serviceTypes";
    CloseableHttpClient httpClient = handle.getHttpClient();
    try (CloseableHttpResponse chart = httpClient.execute(new HttpGet(yarnAppTypesUrl))) {
      JsonNode json = readJsonTree(chart.getEntity().getContent());
      return StreamSupport.stream(json.get("items").spliterator(), false)
          .map(JsonNode::asText)
          .collect(Collectors.toList());
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
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
