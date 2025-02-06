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

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationListDTO;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

public abstract class AbstractClouderaYarnApplicationTask extends AbstractClouderaManagerTask {
  private final ZonedDateTime fromDate;

  public AbstractClouderaYarnApplicationTask(String fileName, int lastDaysToInclude) {
    super(String.format("%s-%dd.jsonl", fileName, lastDaysToInclude));
    Preconditions.checkArgument(
        lastDaysToInclude >= 1,
        String.format("Amount of days must be a positive number. Got %d.", lastDaysToInclude));

    fromDate =
        ZonedDateTime.of(LocalDateTime.now().minusDays(lastDaysToInclude), ZoneId.of("UTC"));
  }

  class PaginatedClouderaYarnApplicationsLoader {
    private static final String ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private final String host;
    private final CloseableHttpClient httpClient;
    private final int limit;
    private int offset;
    private final String fromAppCreationDate;

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle) {
      this(handle, 1000);
    }

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle, int limit) {
      this.host = handle.getApiURI().toString();
      this.httpClient = handle.getHttpClient();
      this.limit = limit;

      final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern(ISO_DATETIME_FORMAT);
      fromAppCreationDate = fromDate.format(dtFormatter);
    }

    public int load(String clusterName, Consumer<List<ApiYARNApplicationDTO>> onPageLoad) {
      return load(clusterName, null, onPageLoad);
    }

    public int load(
        String clusterName,
        @Nullable String appType,
        Consumer<List<ApiYARNApplicationDTO>> onPageLoad) {
      int amountOfLoadedApps = 0;
      offset = 0;
      boolean shouldLoadNext = true;

      while (shouldLoadNext) {
        URI yarnAppsURI = buildNextYARNApplicationPageURI(clusterName, appType);
        List<ApiYARNApplicationDTO> newLoad = load(yarnAppsURI);
        if (!newLoad.isEmpty()) {
          onPageLoad.accept(newLoad);
          offset += limit;
        } else {
          shouldLoadNext = false;
        }
        amountOfLoadedApps += newLoad.size();
      }
      return amountOfLoadedApps;
    }

    private List<ApiYARNApplicationDTO> load(URI yarnAppURI) {
      try (CloseableHttpResponse resp = httpClient.execute(new HttpGet(yarnAppURI))) {
        int statusCode = resp.getStatusLine().getStatusCode();
        if (!isStatusCodeOK(statusCode)) {
          throw new RuntimeException(
              String.format(
                  "Cloudera Error: YARN application API returned HTTP status %d.", statusCode));
        }
        ApiYARNApplicationListDTO yarnAppListDto =
            parseJsonStreamToObject(resp.getEntity().getContent(), ApiYARNApplicationListDTO.class);
        return yarnAppListDto.getApplications();
      } catch (IOException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }

    private URI buildNextYARNApplicationPageURI(String clusterName, @Nullable String appType) {
      String yarnApplicationsUrl =
          host + "clusters/" + clusterName + "/services/yarn/yarnApplications";
      URI yarnApplicationsURI;
      try {
        URIBuilder uriBuilder = new URIBuilder(yarnApplicationsUrl);
        uriBuilder.addParameter("limit", String.valueOf(limit));
        uriBuilder.addParameter("offset", String.valueOf(offset));
        uriBuilder.addParameter("from", fromAppCreationDate);
        if (appType != null) {
          uriBuilder.addParameter("filter", String.format("applicationType=%s", appType));
        }
        yarnApplicationsURI = uriBuilder.build();
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
      return yarnApplicationsURI;
    }
  }
}
