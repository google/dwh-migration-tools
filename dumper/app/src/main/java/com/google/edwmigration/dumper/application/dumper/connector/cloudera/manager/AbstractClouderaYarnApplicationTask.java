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
  private static final DateTimeFormatter isoDateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private final String fromDate;

  public AbstractClouderaYarnApplicationTask(String fileName, int includedLastDays) {
    super(String.format("%s-%dd.jsonl", fileName, includedLastDays));
    Preconditions.checkArgument(
        includedLastDays > 1,
        String.format("Amount of days must be a positive number. Get %d.", includedLastDays));
    this.fromDate = buildISODateTime(includedLastDays);
  }

  private String buildISODateTime(int deltaInDays) {
    ZonedDateTime dateTime =
        ZonedDateTime.of(LocalDateTime.now().minusDays(deltaInDays), ZoneId.of("UTC"));
    return dateTime.format(isoDateTimeFormatter);
  }

  class PaginatedClouderaYarnApplicationsLoader {
    private final String host;
    private final CloseableHttpClient httpClient;
    private final int limit;
    private int offset;

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle) {
      this.host = handle.getApiURI().toString();
      this.httpClient = handle.getHttpClient();
      this.limit = 500;
      this.offset = 0;
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
      boolean nextLoad = true;

      while (nextLoad) {
        URI yarnAppsURI = buildNextYARNApplicationPageURI(clusterName, appType);
        List<ApiYARNApplicationDTO> newLoad = load(yarnAppsURI);
        if (!newLoad.isEmpty()) {
          onPageLoad.accept(newLoad);
        } else {
          nextLoad = false;
        }
        amountOfLoadedApps += newLoad.size();
        offset += limit;
      }
      return amountOfLoadedApps;
    }

    private List<ApiYARNApplicationDTO> load(URI yarnAppURI) {
      try (CloseableHttpResponse resp = httpClient.execute(new HttpGet(yarnAppURI))) {
        final int statusCode = resp.getStatusLine().getStatusCode();
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
        uriBuilder.addParameter("from", fromDate);
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
