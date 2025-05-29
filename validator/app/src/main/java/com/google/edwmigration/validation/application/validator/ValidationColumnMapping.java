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
package com.google.edwmigration.validation.application.validator;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import org.jooq.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ValidationColumnMapping {
  private static final Logger LOG = LoggerFactory.getLogger(ValidationColumnMapping.class);
  private final ImmutableMap<String, String> columnMappings;
  private final HashMap<String, DataType<?>> sourceColumns;
  private final HashMap<String, DataType<?>> targetColumns;
  private final ImmutableMap<String, String> primaryKeys;
  private final List<ColumnEntry> columnEntries = new ArrayList<>();

  public ValidationColumnMapping(
      ImmutableMap<String, String> columnMappings,
      HashMap<String, DataType<?>> sourceColumns,
      HashMap<String, DataType<?>> targetColumns,
      ImmutableMap<String, String> primaryKeys) {
    this.columnMappings = columnMappings;
    this.sourceColumns = sourceColumns;
    this.targetColumns = targetColumns;
    this.primaryKeys = primaryKeys;
    buildColumnEntries();
  }

  public void buildColumnEntries() {
    // Create copy to delete keys as pairs are created
    HashMap<String, DataType<?>> targetSample = new HashMap<>(targetColumns);

    for (Map.Entry<String, DataType<?>> sourceEntry : sourceColumns.entrySet()) {
      String sourceColumnName = sourceEntry.getKey();
      DataType<?> sourceDataType = sourceEntry.getValue();
      String targetColumnName = sourceColumnName;
      boolean isPrimaryKey = false;

      // Check user provided column mappings
      if (columnMappings.containsKey(sourceColumnName)) {
        targetColumnName = columnMappings.get(sourceColumnName);
      }

      // Check if column is a PK
      if (primaryKeys.containsKey(sourceColumnName)) {
        isPrimaryKey = true;
        String targetPkProvided = primaryKeys.get(sourceColumnName);
        if (!Objects.equals(targetPkProvided, targetColumnName)) {
          String errorMsg =
              String.format(
                  "Primary key mapping conflicts with column name mappings for column %s",
                  sourceColumnName);
          LOG.error(errorMsg);
          throw new RuntimeException(errorMsg);
        }
      }

      if (targetSample.containsKey(targetColumnName)) {
        DataType<?> targetDataType = targetSample.get(targetColumnName);
        if (sourceDataType.equals(targetDataType)) {
          ColumnEntry columnEntry =
              new ColumnEntry(
                  sourceColumnName, targetColumnName, sourceDataType, targetDataType, isPrimaryKey);
          columnEntries.add(columnEntry);
          targetSample.remove(targetColumnName);
        } else {
          String errorMsg =
              String.format(
                  "Column mapping found with source column name %s, and target column name %s but different data types found. Source data type: %s, Target data type: %s",
                  sourceColumnName, targetColumnName, sourceDataType, targetDataType);
          LOG.error(errorMsg);
          throw new RuntimeException(errorMsg);
        }
      } else {
        LOG.debug(
            String.format(
                "No matching target column found for source column: %s", sourceColumnName));
        ColumnEntry columnEntry =
            new ColumnEntry(sourceColumnName, null, sourceDataType, null, isPrimaryKey);
        columnEntries.add(columnEntry);
      }
    }

    for (Map.Entry<String, DataType<?>> targetEntry : targetSample.entrySet()) {
      String targetColumnName = targetEntry.getKey();
      DataType<?> targetDataType = targetEntry.getValue();
      boolean isPrimaryKey = false;
      LOG.debug(
          String.format("No matching source column found for target column: %s", targetColumnName));
      if (primaryKeys.containsValue(targetColumnName)) {
        isPrimaryKey = true;
      }
      ColumnEntry columnEntry =
          new ColumnEntry(null, targetColumnName, null, targetDataType, isPrimaryKey);
      columnEntries.add(columnEntry);
    }
  }

  public List<ColumnEntry> getColumnEntries() {
    return columnEntries;
  }

  public static class ColumnEntry {
    private final String sourceColumnName;
    private final String targetColumnName;
    private String sourceColumnAlias = null;
    private String targetColumnAlias = null;
    private final DataType<?> sourceColumnDataType;
    private final DataType<?> targetColumnDataType;
    private boolean isPrimaryKey;
    private final String SOURCE_ALIAS_PREFIX = "s_";
    private final String TARGET_ALIAS_PREFIX = "t_";

    public ColumnEntry(
        @Nullable String sourceColumnName,
        @Nullable String targetColumnName,
        DataType<?> sourceColumnDataType,
        DataType<?> targetColumnDataType,
        boolean isPrimaryKey) {
      this.sourceColumnName = sourceColumnName;
      this.targetColumnName = targetColumnName;
      this.sourceColumnDataType = sourceColumnDataType;
      this.targetColumnDataType = targetColumnDataType;
      this.isPrimaryKey = isPrimaryKey;
      if (sourceColumnName != null) {
        this.sourceColumnAlias = SOURCE_ALIAS_PREFIX + sourceColumnName;
      }
      if (targetColumnName != null) {
        this.targetColumnAlias = TARGET_ALIAS_PREFIX + targetColumnName;
      }
    }

    @CheckForNull
    public String getSourceColumnName() {
      return sourceColumnName;
    }

    @CheckForNull
    public String getTargetColumnName() {
      return targetColumnName;
    }

    public boolean isPrimaryKey() {
      return isPrimaryKey;
    }

    public DataType<?> getSourceColumnDataType() {
      return sourceColumnDataType;
    }

    public DataType<?> getTargetColumnDataType() {
      return targetColumnDataType;
    }

    public String getSourceColumnAlias() {
      return this.sourceColumnAlias;
    }

    public String getTargetColumnAlias() {
      return this.targetColumnAlias;
    }
  }
}
