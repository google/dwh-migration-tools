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
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.auto.value.AutoValue;
import java.time.LocalDateTime;
import javax.annotation.Nullable;

@AutoValue
@JsonSerialize(as = HdfsPermission.class)
public abstract class HdfsPermission {

  public static final CsvMapper CSV_MAPPER =
      (CsvMapper) new CsvMapper().registerModule(new JavaTimeModule());

  @JsonCreator
  public static HdfsPermission create(
      @JsonProperty("Path") String path,
      @JsonProperty("FileType") String fileType,
      @JsonProperty("FileSize") Long fileSize,
      @JsonProperty("Owner") String owner,
      @JsonProperty("Group") String group,
      @JsonProperty("Permission") String permission,
      @JsonProperty("ModificationTime") LocalDateTime modificationTime,
      @JsonProperty("FileCount") Long fileCount,
      @JsonProperty("DirCount") Long dirCount,
      @JsonProperty("StoragePolicy") String storagePolicy) {
    return new AutoValue_HdfsPermission(
        path,
        fileType,
        fileSize,
        owner,
        group,
        permission,
        modificationTime,
        fileCount,
        dirCount,
        storagePolicy);
  }

  @JsonProperty("Path")
  public abstract String path();

  @JsonProperty("FileType")
  @Nullable
  public abstract String fileType();

  @JsonProperty("FileSize")
  @Nullable
  public abstract Long fileSize();

  @JsonProperty("Owner")
  @Nullable
  public abstract String owner();

  @JsonProperty("Group")
  @Nullable
  public abstract String group();

  @JsonProperty("Permission")
  @Nullable
  public abstract String permission();

  @JsonProperty("ModificationTime")
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
  @Nullable
  public abstract LocalDateTime modificationTime();

  @JsonProperty("FileCount")
  @Nullable
  public abstract Long fileCount();

  @JsonProperty("DirCount")
  @Nullable
  public abstract Long dirCount();

  @JsonProperty("StoragePolicy")
  @Nullable
  public abstract String storagePolicy();
}
