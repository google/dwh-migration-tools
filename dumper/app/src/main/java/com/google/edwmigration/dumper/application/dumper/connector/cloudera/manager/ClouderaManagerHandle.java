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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.http.impl.client.CloseableHttpClient;

@ThreadSafe
public class ClouderaManagerHandle implements Handle {
  private final URI apiURI;
  private final CloseableHttpClient httpClient;

  private ImmutableList<ClouderaClusterDTO> clusters;

  public ClouderaManagerHandle(URI apiURI, CloseableHttpClient httpClient) {
    this.apiURI = apiURI;
    this.httpClient = httpClient;
  }

  public URI getApiURI() {
    return apiURI;
  }

  public URI getBaseURI() {
    return apiURI.resolve("/");
  }

  public CloseableHttpClient getHttpClient() {
    return httpClient;
  }

  @CheckForNull
  public synchronized ImmutableList<ClouderaClusterDTO> getClusters() {
    return clusters;
  }

  public synchronized void initClusters(List<ClouderaClusterDTO> clusters) {
    if (this.clusters != null) {
      throw new IllegalStateException("The cluster already initialized!");
    }
    this.clusters = ImmutableList.copyOf(clusters);
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException ignore) {

      }
    }
  }

  public static class ClouderaClusterDTO {

    private final String id;
    private final String name;

    public ClouderaClusterDTO(String id, String name) {
      this.id = id;
      this.name = name;
    }

    @CheckForNull
    public String getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }
}
