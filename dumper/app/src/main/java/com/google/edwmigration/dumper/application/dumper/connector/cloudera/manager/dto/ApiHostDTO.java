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

/**
 * DTO class for the official Cloudera API <a
 * href="https://archive.cloudera.com/cm7/7.11.3.0/generic/jar/cm_api/apidocs/json_ApiHost.html">json</a>
 *
 * <p>Note: The license for <a
 * href="https://mvnrepository.com/artifact/com.cloudera.api.swagger/cloudera-manager-api-swagger/7.11.0">generated</a>
 * code is unclear, the own model for public schema used instead of it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApiHostDTO {

  @JsonProperty(required = true)
  private String hostname;

  @JsonProperty(required = true)
  private String hostId;

  public String getName() {
    return hostname;
  }

  public String getId() {
    return hostId;
  }
}
