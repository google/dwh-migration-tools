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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class AbstractClouderaYarnApplicationTask extends AbstractClouderaManagerTask {
  private final ZonedDateTime fromDate;
  private final ZonedDateTime toDate;
  private final TaskCategory taskCategory;

  public AbstractClouderaYarnApplicationTask(
      @Nonnull String targetPath,
      @Nonnull ZonedDateTime startDate,
      @Nonnull ZonedDateTime endDate,
      @Nonnull TaskCategory taskCategory) {
    super(targetPath);
    Preconditions.checkNotNull(startDate, "Start date must be not null.");
    Preconditions.checkNotNull(endDate, "End date must be not null.");
    Preconditions.checkNotNull(taskCategory, "Task category must be not null.");
    Preconditions.checkState(startDate.isBefore(endDate), "Start Date has to be before End Date.");

    fromDate = startDate;
    toDate = endDate;
    this.taskCategory = taskCategory;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return taskCategory;
  }

  class PaginatedClouderaYarnApplicationsLoader {
    private static final String ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private final URI apiURI;
    private final CloseableHttpClient httpClient;
    private final int limit;
    private int offset;
    private final String fromAppCreationDate;
    private final String toAppCreationDate;

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle, int limit) {
      this.apiURI = handle.getApiURI();
      this.httpClient = handle.getHttpClient();
      this.limit = limit;

      final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(ISO_DATETIME_FORMAT);
      fromAppCreationDate = fromDate.format(dtFormatter);
      toAppCreationDate = toDate.format(dtFormatter);
    }

    public int load(String clusterName, Consumer<List<ApiYARNApplicationDTO>> onPageLoad) {
      return load(clusterName, null, onPageLoad);
    }

    public int load(
        String clusterName,
        @Nullable String appType,
        Consumer<List<ApiYARNApplicationDTO>> onPageLoad) {
      offset = 0;
      boolean fetchedNewApps;
      do {
        fetchedNewApps = false;
        URI yarnAppsURI = buildNextYARNApplicationPageURI(clusterName, appType);
        List<ApiYARNApplicationDTO> newLoad = load(yarnAppsURI);
        if (!newLoad.isEmpty()) {
          onPageLoad.accept(newLoad);
          offset += newLoad.size();
          fetchedNewApps = true;
        }
      } while (fetchedNewApps);
      return offset;
    }

    private List<ApiYARNApplicationDTO> load(URI yarnAppURI) {
      try (CloseableHttpResponse resp = httpClient.execute(new HttpGet(yarnAppURI))) {
        int statusCode = resp.getStatusLine().getStatusCode();
        if (!isStatusCodeOK(statusCode)) {
          throw new ClouderaConnectorException(
              String.format(
                  "YARN application API returned HTTP status %d. Message: %s",
                  statusCode, readFromStream(resp.getEntity().getContent())));
        }

        JsonNode applicationsResponse = readJsonTree(resp.getEntity().getContent());
        JsonNode applicationsArray = applicationsResponse.at("/applications");
        if (!applicationsArray.isArray()) {
          throw new IllegalArgumentException(
              "Unexpected JSON response without `applications`. "
                  + "Response: "
                  + applicationsResponse);
        }
        return toDTOs((ArrayNode) applicationsArray);
      } catch (IOException ex) {
        throw new ClouderaConnectorException(ex.getMessage(), ex);
      }
    }

    private List<ApiYARNApplicationDTO> toDTOs(ArrayNode applicationsArray) {
      List<ApiYARNApplicationDTO> yarnApplicationDTOs = new ArrayList<>();
      for (JsonNode application : applicationsArray) {
        yarnApplicationDTOs.add(new ApiYARNApplicationDTO(application));
      }

      return yarnApplicationDTOs;
    }

    private URI buildNextYARNApplicationPageURI(String clusterName, @Nullable String appType) {
      try {
        URIBuilder uriBuilder =
            new URIBuilder()
                .setPathSegments("clusters", clusterName, "services", "yarn", "yarnApplications")
                .addParameter("limit", String.valueOf(limit))
                .addParameter("offset", String.valueOf(offset))
                .addParameter("from", fromAppCreationDate)
                .addParameter("to", toAppCreationDate);
        if (appType != null) {
          uriBuilder.addParameter("filter", String.format("applicationType=\"%s\"", appType));
        }
        return new URI(apiURI + uriBuilder.build().toString());
      } catch (URISyntaxException ex) {
        throw new ClouderaConnectorException("Can't build Cloudera endpoint.", ex);
      }
    }
  }
}
