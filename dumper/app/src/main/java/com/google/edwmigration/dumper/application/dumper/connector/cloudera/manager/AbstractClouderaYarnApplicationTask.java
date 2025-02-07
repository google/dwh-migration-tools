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

    fromDate = ZonedDateTime.of(LocalDateTime.now().minusDays(lastDaysToInclude), ZoneId.of("UTC"));
  }

  class PaginatedClouderaYarnApplicationsLoader {
    private static final String ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private final URI host;
    private final CloseableHttpClient httpClient;
    private final int limit;
    private int offset;
    private final String fromAppCreationDate;

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle) {
      this(handle, 1000);
    }

    public PaginatedClouderaYarnApplicationsLoader(ClouderaManagerHandle handle, int limit) {
      this.host = handle.getApiURI();
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
                  statusCode, resp.getEntity().getContent().toString()));
        }
        ApiYARNApplicationListDTO yarnAppListDto =
            parseJsonStreamToObject(resp.getEntity().getContent(), ApiYARNApplicationListDTO.class);
        return yarnAppListDto.getApplications();
      } catch (IOException ex) {
        throw new ClouderaConnectorException(ex.getMessage(), ex);
      }
    }

    private URI buildNextYARNApplicationPageURI(String clusterName, @Nullable String appType) {
      try {
        URIBuilder uriBuilder =
            new URIBuilder()
                .setPathSegments("clusters", clusterName, "services", "yarn", "yarnApplications")
                .addParameter("limit", String.valueOf(limit))
                .addParameter("offset", String.valueOf(offset))
                .addParameter("from", fromAppCreationDate);
        if (appType != null) {
          uriBuilder.addParameter("filter", String.format("applicationType=%s", appType));
        }
        return new URI(host + uriBuilder.build().toString());
      } catch (URISyntaxException ex) {
        throw new ClouderaConnectorException("Can't build Cloudera endpoint.", ex);
      }
    }
  }
}
