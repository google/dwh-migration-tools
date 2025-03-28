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
package com.google.edwmigration.permissions.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = Table.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public abstract class Table {

  @JsonCreator
  public static Table create(
      @JsonProperty("name") String name,
      @JsonProperty("schema_name") String schemaName,
      @JsonProperty("hdfs_path") String hdfsPath,
      @JsonProperty("gcs_path") String gcsPath,
      @JsonProperty("bq_resource") String bqResource) {
    return new AutoValue_Table(name, schemaName, hdfsPath, gcsPath, bqResource);
  }

  @JsonProperty
  public abstract String name();

  @JsonProperty
  public abstract String schemaName();

  @JsonProperty
  public abstract String hdfsPath();

  @JsonProperty
  @Nullable
  public abstract String gcsPath();

  @JsonProperty
  @Nullable
  public abstract String bqTable();

  @JsonIgnore
  public String fullName() {
    return String.format("%s.%s", schemaName(), name());
  }
}
