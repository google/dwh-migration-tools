/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.ext.hive.metastore;

import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public interface Table {

  @CheckForNull
  public String getDatabaseName();

  @CheckForNull
  public String getTableName();

  @CheckForNull
  public String getTableType();

  @CheckForNull
  public Integer getCreateTime();

  @CheckForNull
  public Integer getLastAccessTime();

  @CheckForNull
  public String getOwner();

  @CheckForNull
  public String getOriginalViewText();

  @CheckForNull
  public String getExpandedViewText();

  @CheckForNull
  public String getLocation();

  @CheckForNull
  public Integer getLastDdlTime();

  @CheckForNull
  public Long getTotalSize();

  @CheckForNull
  public Long getRawSize();

  @CheckForNull
  public Long getRowsCount();

  @CheckForNull
  public Integer getFilesCount();

  @CheckForNull
  public Integer getRetention();

  @CheckForNull
  public Integer getBucketsCount();

  @CheckForNull
  public Boolean isCompressed();

  @Nonnull
  public List<? extends Field> getFields() throws Exception;

  @Nonnull
  public List<? extends PartitionKey> getPartitionKeys();

  @Nonnull
  public List<? extends Partition> getPartitions() throws Exception;
}
