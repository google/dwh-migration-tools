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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.util.StringUtils;

/**
 * DTO class for the official Cloudera API <a
 * href="https://archive.cloudera.com/cm7/7.0.3/generic/jar/cm_api/apidocs/json_ApiYarnApplication.html">json</a>
 * with additional information about cluster.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiYarnApplicationDto {

  @JsonProperty(required = true)
  private String clusterName;

  /**
   * DTO class for the official Cloudera API <a
   * href="https://archive.cloudera.com/cm7/7.0.3/generic/jar/cm_api/apidocs/json_ApiYarnApplication.html">json</a>.
   * JsonNode used to support schema evaluation.
   */
  private final JsonNode apiYarnApplication;

  public ApiYarnApplicationDto(JsonNode apiYarnApplication) {
    this.apiYarnApplication = apiYarnApplication;
  }

  @JsonIgnore
  public String getApplicationId() {
    if (apiYarnApplication == null) {
      return null;
    }
    String applicationId = apiYarnApplication.at("/applicationId").asText();
    return StringUtils.hasText(applicationId) ? applicationId : null;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public JsonNode getApiYarnApplication() {
    return apiYarnApplication;
  }
}
