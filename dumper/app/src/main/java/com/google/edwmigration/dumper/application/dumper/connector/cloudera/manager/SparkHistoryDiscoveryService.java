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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiConfigDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiConfigListDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiRoleDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiRoleListDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiServiceDTO;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiServiceListDTO;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkHistoryDiscoveryService {

  private static final Logger logger = LoggerFactory.getLogger(SparkHistoryDiscoveryService.class);

  private static final List<String> CANDIDATE_PATHS =
      ImmutableList.of("spark3history", "sparkhistory");

  private final ObjectMapper objectMapper;
  private final CloseableHttpClient cmClient;
  private final URI apiURI;

  public SparkHistoryDiscoveryService(
      ObjectMapper objectMapper, CloseableHttpClient cmClient, URI apiURI) {
    this.objectMapper = objectMapper;
    this.cmClient = cmClient;
    this.apiURI = apiURI;
  }

  /**
   * Discovers the active Spark History Server URL. Probes Spark 3 and Spark 2 endpoints to see
   * which one is alive.
   */
  public Optional<String> resolveUrl(String clusterName, CloseableHttpClient knoxClient) {
    try {
      Optional<KnoxGatewayInfo> knoxInfo = getKnoxGatewayInfo(clusterName);

      if (!knoxInfo.isPresent()) {
        logger.warn("Could not find Knox Service or Topology for cluster: {}", clusterName);
        return Optional.empty();
      }

      KnoxGatewayInfo info = knoxInfo.get();

      for (String context : CANDIDATE_PATHS) {
        String candidateUrl =
            String.format(
                "https://%s/%s/%s/%s/api/v1",
                info.hostname, info.gatewayPath, info.topologyName, context);

        if (isReachable(candidateUrl, knoxClient)) {
          logger.info("Found active Spark History Server at: {}", candidateUrl);
          return Optional.of(candidateUrl);
        }
      }
      logger.warn(
          "Knox is active, but neither Spark 2 nor Spark 3 history endpoints are reachable.");
    } catch (IOException e) {
      logger.warn(
          "Cluster '{}': Failed to discover Spark History URL due to error: {}",
          clusterName,
          e.getMessage());
    }
    return Optional.empty();
  }

  private boolean isReachable(String url, CloseableHttpClient knoxClient) {
    String probeUrl = url + "/applications?limit=1";

    try (CloseableHttpResponse response = knoxClient.execute(new HttpGet(probeUrl))) {
      int status = response.getStatusLine().getStatusCode();
      return status == 200;
    } catch (IOException e) {
      logger.debug("Probe failed for URL: {}", url);
      return false;
    }
  }

  private Optional<KnoxGatewayInfo> getKnoxGatewayInfo(String clusterName) throws IOException {
    Optional<String> knoxServiceName = getKnoxServiceName(clusterName);
    if (!knoxServiceName.isPresent()) {
      return Optional.empty();
    }

    Optional<ApiRoleDTO> knoxRole = getKnoxRole(clusterName, knoxServiceName.get());
    if (!knoxRole.isPresent()) {
      return Optional.empty();
    }

    ApiRoleDTO role = knoxRole.get();
    if (role.getHostRef() == null || role.getRoleConfigGroupRef() == null) {
      logger.warn("Knox role is missing hostRef or roleConfigGroupRef.");
      return Optional.empty();
    }

    String hostname = role.getHostRef().getHostname();
    String roleConfigGroup = role.getRoleConfigGroupRef().getRoleConfigGroupName();
    if (hostname == null || roleConfigGroup == null) {
      logger.warn("Knox role has null hostname or roleConfigGroupName.");
      return Optional.empty();
    }

    String gatewayPath = getGatewayPath(clusterName, knoxServiceName.get(), roleConfigGroup);
    String topologyName = getTopologyName(clusterName, knoxServiceName.get(), roleConfigGroup);

    return Optional.of(new KnoxGatewayInfo(hostname, gatewayPath, topologyName));
  }

  private Optional<String> getKnoxServiceName(String clusterName) throws IOException {
    String path = String.format("clusters/%s/services", clusterName);
    Optional<ApiServiceListDTO> serviceList = get(path, ApiServiceListDTO.class);
    return serviceList.flatMap(
        list ->
            list.getItems().stream()
                .filter(service -> "KNOX".equals(service.getType()))
                .map(ApiServiceDTO::getName)
                .findFirst());
  }

  private Optional<ApiRoleDTO> getKnoxRole(String clusterName, String knoxServiceName)
      throws IOException {
    String path = String.format("clusters/%s/services/%s/roles", clusterName, knoxServiceName);
    Optional<ApiRoleListDTO> roleList = get(path, ApiRoleListDTO.class);
    return roleList.flatMap(list -> list.getItems().stream().findFirst());
  }

  private String getGatewayPath(
      String clusterName, String knoxServiceName, String roleConfigGroupName) throws IOException {
    return getConfigValue(clusterName, knoxServiceName, roleConfigGroupName, "gateway_path")
        .orElse(clusterName);
  }

  private String getTopologyName(
      String clusterName, String knoxServiceName, String roleConfigGroupName) throws IOException {
    return getConfigValue(
            clusterName, knoxServiceName, roleConfigGroupName, "gateway_default_api_topology_name")
        .orElse("cdp-proxy-api");
  }

  private Optional<String> getConfigValue(
      String clusterName, String knoxServiceName, String roleConfigGroupName, String configName)
      throws IOException {
    String path =
        String.format(
            "clusters/%s/services/%s/roleConfigGroups/%s/config",
            clusterName, knoxServiceName, roleConfigGroupName);
    Optional<ApiConfigListDTO> configList = get(path, ApiConfigListDTO.class);
    return configList.flatMap(
        list ->
            list.getItems().stream()
                .filter(config -> configName.equals(config.getName()))
                .map(ApiConfigDTO::getValue)
                .findFirst());
  }

  public <T> Optional<T> get(String path, Class<T> responseType) throws IOException {
    URI requestUri = apiURI.resolve(path);
    try (CloseableHttpResponse response = cmClient.execute(new HttpGet(requestUri))) {
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new IOException(
            "Unexpected status code "
                + response.getStatusLine().getStatusCode()
                + " from "
                + requestUri);
      }
      HttpEntity entity = response.getEntity();
      if (entity == null) {
        return Optional.empty();
      }
      return Optional.of(objectMapper.readValue(entity.getContent(), responseType));
    }
  }

  private static class KnoxGatewayInfo {
    String hostname;
    String gatewayPath;
    String topologyName;

    KnoxGatewayInfo(String hostname, String gatewayPath, String topologyName) {
      this.hostname = hostname;
      this.gatewayPath = gatewayPath;
      this.topologyName = topologyName;
    }
  }
}
