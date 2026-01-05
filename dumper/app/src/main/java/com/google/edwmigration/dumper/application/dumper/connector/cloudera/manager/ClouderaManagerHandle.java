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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.http.impl.client.CloseableHttpClient;

@ThreadSafe
public class ClouderaManagerHandle implements Handle {

  private final URI apiURI;
  private final CloseableHttpClient httpClient;

  private ImmutableList<ClouderaClusterDTO> clusters;
  private ImmutableList<ClouderaHostDTO> hosts;
  private ImmutableList<ClouderaYarnApplicationDTO> sparkYarnApplications;

  public ClouderaManagerHandle(URI apiURI, CloseableHttpClient httpClient) {
    Preconditions.checkNotNull(apiURI, "Cloudera's apiURI can't be null.");
    Preconditions.checkNotNull(httpClient, "httpClient can't be null.");

    // Always add trailing slash for safety
    this.apiURI = unify(apiURI);
    this.httpClient = httpClient;
  }

  /** 1. Remove query params and url fragments 2. Add trailing slash for safety */
  private static URI unify(URI uri) {
    try {
      return new URI(
              uri.getScheme(),
              uri.getUserInfo(),
              uri.getHost(),
              uri.getPort(),
              uri.getPath() + "/",
              null,
              null)
          .normalize();
    } catch (URISyntaxException e) {
      throw new RuntimeException("URI must be valid at this stage", e);
    }
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
    Preconditions.checkNotNull(clusters, "Clusters can't be initialised to null list.");
    Preconditions.checkArgument(
        !clusters.isEmpty(), "Clusters can't be initialised to empty list.");
    Preconditions.checkState(this.clusters == null, "The cluster already initialized.");

    this.clusters = ImmutableList.copyOf(clusters);
  }

  @CheckForNull
  public synchronized ImmutableList<ClouderaHostDTO> getHosts() {
    return hosts;
  }

  public synchronized void initHosts(List<ClouderaHostDTO> hosts) {
    Preconditions.checkNotNull(hosts, "Hosts can't be initialised to null list.");
    Preconditions.checkArgument(!hosts.isEmpty(), "Hosts can't be initialised to empty list.");
    Preconditions.checkState(this.hosts == null, "Hosts already initialized.");

    this.hosts = ImmutableList.copyOf(hosts);
  }

  @CheckForNull
  public synchronized ImmutableList<ClouderaYarnApplicationDTO> getSparkYarnApplications() {
    return sparkYarnApplications;
  }

  public synchronized void initSparkYarnApplications(
      List<ClouderaYarnApplicationDTO> sparkYarnApplications) {
    Preconditions.checkNotNull(
        sparkYarnApplications, "Spark YARN applications can't be initialised to null list.");
    Preconditions.checkState(
        this.sparkYarnApplications == null, "Spark YARN applications already initialized.");

    this.sparkYarnApplications = ImmutableList.copyOf(sparkYarnApplications);
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException ignore) {
        // The intention is to do graceful shutdown and try to release the resource.
        // In case of errors we do not need to interrupt the execution flow
        // because the e2e use case might be successful
      }
    }
  }

  @AutoValue
  public abstract static class ClouderaClusterDTO {
    public static ClouderaClusterDTO create(String id, String name) {
      return new AutoValue_ClouderaManagerHandle_ClouderaClusterDTO(id, name);
    }

    @CheckForNull
    @Nullable
    abstract String getId();

    abstract String getName();
  }

  @AutoValue
  public abstract static class ClouderaHostDTO {
    public static ClouderaHostDTO create(String id, String name) {
      return new AutoValue_ClouderaManagerHandle_ClouderaHostDTO(id, name);
    }

    @CheckForNull
    @Nullable
    abstract String getId();

    abstract String getName();
  }

  @AutoValue
  public abstract static class ClouderaYarnApplicationDTO {
    public static ClouderaYarnApplicationDTO create(String id, String clusterName) {
      return new AutoValue_ClouderaManagerHandle_ClouderaYarnApplicationDTO(id, clusterName);
    }

    @CheckForNull
    @Nullable
    abstract String getId();

    abstract String getClusterName();
  }
}
