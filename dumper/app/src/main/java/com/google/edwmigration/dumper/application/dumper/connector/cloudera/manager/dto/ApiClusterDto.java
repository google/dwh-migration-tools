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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * DTO class for the official Cloudera API <a
 * href="https://archive.cloudera.com/cm7/7.11.3.0/generic/jar/cm_api/apidocs/json_ApiCluster.html">json</a>
 *
 * <p>Note: The license for <a
 * href="https://mvnrepository.com/artifact/com.cloudera.api.swagger/cloudera-manager-api-swagger/7.11.0">generated</a>
 * code is unclear, the own model for public schema used instead of it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiClusterDto {

  @JsonProperty(required = true)
  private String name;

  private String displayName;
  private String fullVersion;
  private Boolean maintenanceMode;
  private List<String> maintenanceOwners;
  private String clusterUrl;
  private String hostsUrl;
  private String entityStatus;
  private String uuid;
  private String clusterType;

  private List<ApiEntityTag> tags;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getFullVersion() {
    return fullVersion;
  }

  public void setFullVersion(String fullVersion) {
    this.fullVersion = fullVersion;
  }

  public Boolean getMaintenanceMode() {
    return maintenanceMode;
  }

  public void setMaintenanceMode(Boolean maintenanceMode) {
    this.maintenanceMode = maintenanceMode;
  }

  public List<String> getMaintenanceOwners() {
    return maintenanceOwners;
  }

  public void setMaintenanceOwners(List<String> maintenanceOwners) {
    this.maintenanceOwners = maintenanceOwners;
  }

  public String getClusterUrl() {
    return clusterUrl;
  }

  public void setClusterUrl(String clusterUrl) {
    this.clusterUrl = clusterUrl;
  }

  public String getHostsUrl() {
    return hostsUrl;
  }

  public void setHostsUrl(String hostsUrl) {
    this.hostsUrl = hostsUrl;
  }

  public String getEntityStatus() {
    return entityStatus;
  }

  public void setEntityStatus(String entityStatus) {
    this.entityStatus = entityStatus;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getClusterType() {
    return clusterType;
  }

  public void setClusterType(String clusterType) {
    this.clusterType = clusterType;
  }

  public List<ApiEntityTag> getTags() {
    return tags;
  }

  public void setTags(List<ApiEntityTag> tags) {
    this.tags = tags;
  }
}
