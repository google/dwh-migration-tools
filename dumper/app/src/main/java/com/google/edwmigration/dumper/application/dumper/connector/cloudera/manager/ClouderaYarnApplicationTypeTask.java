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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaClusterDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.ClouderaManagerHandle.ClouderaYarnApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model.YarnApplicationType;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClouderaYarnApplicationTypeTask extends AbstractClouderaYarnApplicationTask {
  private static final Logger logger =
      LoggerFactory.getLogger(ClouderaYarnApplicationTypeTask.class);

  public ClouderaYarnApplicationTypeTask(
      ZonedDateTime startDate, ZonedDateTime endDate, TaskCategory taskCategory) {
    super("yarn-application-types.jsonl", startDate, endDate, taskCategory);
  }

  @Override
  protected void doRun(
      TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle)
      throws Exception {
    List<ClouderaClusterDTO> clusters = handle.getClusters();
    try {
      Preconditions.checkNotNull(
          clusters, "Clusters must be initialized before fetching YARN application types.");
    } catch (NullPointerException ex) {
      MetadataDumperUsageException newEx = new MetadataDumperUsageException(ex.getMessage());
      newEx.initCause(ex);
      throw newEx;
    }

    PaginatedClouderaYarnApplicationsLoader appLoader =
        new PaginatedClouderaYarnApplicationsLoader(
            handle, context.getArguments().getPaginationPageSize());

    List<ClouderaYarnApplicationDTO> sparkYarnApplications = new ArrayList<>();
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream()) {
      for (ClouderaClusterDTO cluster : clusters) {
        final String clusterName = cluster.getName();
        for (String yarnAppType : collectYarnApplicationTypes(context, handle, clusterName)) {
          logger.info(
              "Dump YARN applications with {} type from {} cluster", yarnAppType, clusterName);
          int loadedAppsCount =
              appLoader.load(
                  clusterName,
                  yarnAppType,
                  yarnAppsPage -> {
                    writeYarnAppTypes(writer, yarnAppsPage, yarnAppType, clusterName);
                    if (yarnAppType.equals(YarnApplicationType.SPARK.getValue())) {
                      yarnAppsPage.stream()
                          .map(
                              yarnApp ->
                                  ClouderaYarnApplicationDTO.create(
                                      yarnApp.getApplicationId(), clusterName))
                          .forEach(sparkYarnApplications::add);
                    }
                  });
          logger.info(
              "Dumped {} YARN applications with {} type from {} cluster",
              loadedAppsCount,
              yarnAppType,
              clusterName);
        }
      }
    }
    handle.initSparkYarnApplications(sparkYarnApplications);
  }

  private void writeYarnAppTypes(
      Writer writer, List<ApiYARNApplicationDTO> yarnApps, String appType, String clusterName) {
    List<ApplicationTypeToYarnApplication> yarnAppTypeMappings = new ArrayList<>();
    for (ApiYARNApplicationDTO yarnApp : yarnApps) {
      yarnAppTypeMappings.add(
          new ApplicationTypeToYarnApplication(yarnApp.getApplicationId(), appType, clusterName));
    }
    try {
      String yarnAppTypeMappingsInJson =
          serializeObjectToJsonString(ImmutableMap.of("yarnAppTypes", yarnAppTypeMappings));
      writer.write(yarnAppTypeMappingsInJson);
      writer.write('\n');
    } catch (IOException ex) {
      throw new ClouderaConnectorException("Can't write YARN application types", ex);
    }
  }

  private Set<String> collectYarnApplicationTypes(
      TaskRunContext context, ClouderaManagerHandle handle, String clusterName) {
    Set<String> yarnApplicationTypes = new HashSet<>();
    ImmutableList<String> predefinedYarnAppTypes =
        stream(YarnApplicationType.values())
            .map(YarnApplicationType::getValue)
            .collect(toImmutableList());
    yarnApplicationTypes.addAll(predefinedYarnAppTypes);
    yarnApplicationTypes.addAll(fetchClusterServiceTypes(handle, clusterName));
    yarnApplicationTypes.addAll(context.getArguments().getYarnApplicationTypes());
    return yarnApplicationTypes;
  }

  private ImmutableList<String> fetchClusterServiceTypes(
      ClouderaManagerHandle handle, String clusterName) {
    String serviceTypesUrl =
        handle.getApiURI().toString() + "clusters/" + clusterName + "/serviceTypes";
    CloseableHttpClient httpClient = handle.getHttpClient();
    try (CloseableHttpResponse serviceTypesResp =
        httpClient.execute(new HttpGet(serviceTypesUrl))) {
      int statusCode = serviceTypesResp.getStatusLine().getStatusCode();
      if (!isStatusCodeOK(statusCode)) {
        throw new ClouderaConnectorException(
            String.format(
                "Cloudera API returned bad http status: %d. Message: %s",
                statusCode, readFromStream(serviceTypesResp.getEntity().getContent())));
      }
      JsonNode serviceTypesJson = readJsonTree(serviceTypesResp.getEntity().getContent());
      return StreamSupport.stream(serviceTypesJson.get("items").spliterator(), false)
          .map(JsonNode::asText)
          .collect(toImmutableList());
    } catch (IOException ex) {
      throw new ClouderaConnectorException(ex.getMessage(), ex);
    }
  }

  private static class ApplicationTypeToYarnApplication {
    private final String applicationId;
    private final String applicationType;
    private final String clusterName;

    public ApplicationTypeToYarnApplication(
        String applicationId, String applicationType, String clusterName) {
      this.applicationId = applicationId;
      this.applicationType = applicationType;
      this.clusterName = clusterName;
    }

    public String getApplicationId() {
      return applicationId;
    }

    public String getApplicationType() {
      return applicationType;
    }

    public String getClusterName() {
      return clusterName;
    }
  }
}
