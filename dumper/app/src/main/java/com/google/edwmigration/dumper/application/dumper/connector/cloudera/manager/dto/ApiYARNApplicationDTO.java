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
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiYARNApplicationDTO {
  @JsonProperty(required = true)
  private String clusterName;

  @JsonProperty(required = true)
  private String applicationId;

  @JsonProperty(required = true)
  private String applicationType;

  @JsonProperty(required = true)
  private String name;

  @JsonProperty(required = true)
  private String state;

  @JsonProperty(required = true)
  private String startTime;

  @JsonProperty(required = true)
  private String endTime;

  @JsonProperty(required = true)
  private String user;

  @JsonProperty(required = true)
  private String pool;

  @JsonProperty(required = true)
  private List<String> applicationTags;

  @JsonProperty(required = true)
  private long allocatedMemorySeconds;

  @JsonProperty(required = true)
  private long allocatedVcoreSeconds;

  @JsonProperty(required = true)
  private Map<String, String> attributes;

  public String getClusterName() {
    return clusterName;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public String getApplicationType() {
    return applicationType;
  }

  public String getName() {
    return name;
  }

  public String getState() {
    return state;
  }

  public String getStartTime() {
    return startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public String getUser() {
    return user;
  }

  public String getPool() {
    return pool;
  }

  public List<String> getApplicationTags() {
    return applicationTags;
  }

  public long getAllocatedMemorySeconds() {
    return allocatedMemorySeconds;
  }

  public long getAllocatedVcoreSeconds() {
    return allocatedVcoreSeconds;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public void setApplicationType(String applicationType) {
    this.applicationType = applicationType;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPool(String pool) {
    this.pool = pool;
  }

  public void setApplicationTags(List<String> applicationTags) {
    this.applicationTags = applicationTags;
  }

  public void setAllocatedMemorySeconds(long allocatedMemorySeconds) {
    this.allocatedMemorySeconds = allocatedMemorySeconds;
  }

  public void setAllocatedVcoreSeconds(long allocatedVcoreSeconds) {
    this.allocatedVcoreSeconds = allocatedVcoreSeconds;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }
}
