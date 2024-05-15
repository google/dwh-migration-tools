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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.List;
import javax.annotation.CheckForNull;

/** @author matt */
public interface BigQueryMetadataDumpFormat {

  public static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
          .disable(SerializationFeature.INDENT_OUTPUT);
  String FORMAT_NAME = "bigquery.dump.zip";

  public static enum TimePartitioningType {
    // See BQ's TimePartitioning.Type
    HOUR,
    DAY,
    MONTH,
    YEAR;
  }

  public static interface DatasetsTaskFormat {

    String ZIP_ENTRY_NAME = "datasets.csv";

    enum Header {
      ProjectId,
      DatasetId,
      DatasetFriendlyName,
      DatasetLocation
    }
  }

  public static interface TablesJsonTaskFormat {

    public static final String ZIP_ENTRY_NAME = "tables.jsonl";

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public static class Metadata {

      @CheckForNull
      public static Metadata fromJson(@CheckForNull String text) throws IOException {
        if (text == null) return null;
        if (text.isEmpty()) return null;
        return MAPPER.readValue(text, Metadata.class);
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
      @JsonInclude(JsonInclude.Include.NON_ABSENT)
      public static class Field {

        public static enum Mode {
          NULLABLE,
          REQUIRED,
          REPEATED
        }

        public String name;
        /** LegacySQLTypeName. */
        public String type;

        public List<Field> subFields;
        public String mode;
        public String description;
      }

      @CheckForNull public String project;
      @CheckForNull public String dataset;
      @CheckForNull public String table;
      @CheckForNull public String friendlyName;
      // TABLE, VIEW, EXTERNAL, (MODEL?)
      @CheckForNull public String tableType;
      @CheckForNull public List<Field> schema;
      @CheckForNull public String timePartitioningField;
      @CheckForNull public TimePartitioningType timePartitioningType = TimePartitioningType.DAY;
      public boolean timePartitioningRequired;
      @CheckForNull public String viewQuery;
      /** In milliseconds, since the epoch. */
      @CheckForNull public Long creationTime;
      /** In milliseconds, since the epoch. Null means the table does not expire. */
      @CheckForNull public Long expirationTime;
    }
  }

  @Deprecated // Use TablesJsonTaskFormat.
  public static interface TablesTaskFormat {

    String ZIP_ENTRY_NAME = "tables.csv";

    public static class Metadata {

      @CheckForNull
      public static Metadata fromJson(@CheckForNull String text) throws IOException {
        if (text == null) return null;
        if (text.isEmpty()) return null;
        return MAPPER.readValue(text, Metadata.class);
      }

      @CheckForNull public String viewQuery;
      @CheckForNull public String timePartitioningField;
      @CheckForNull public TimePartitioningType timePartitioningType = TimePartitioningType.DAY;
      public boolean timePartitioningRequired;
    }

    public static enum Header {
      ProjectId,
      DatasetId,
      TableId,
      TableFriendlyName,
      TableType,
      NumRows,
      NumBytes,
      TableMetadata
    }
  }

  @Deprecated // Use TablesJsonTaskFormat.
  public static interface ColumnsTaskFormat {

    String ZIP_ENTRY_NAME = "fields.csv";

    /**
     * This isn't actually much use on a column, because of the pseudo-fields _PARTITIONDATE and
     * _PARTITIONTIME.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public static class Metadata {

      @CheckForNull
      public static Metadata fromJson(@CheckForNull String text) throws IOException {
        if (text == null) return null;
        if (text.isEmpty()) return null;
        return MAPPER.readValue(text, Metadata.class);
      }

      @JsonProperty @CheckForNull
      public TimePartitioningType timePartitioningType = TimePartitioningType.DAY;

      @JsonProperty public boolean timePartitioningRequired;
    }

    public static enum Header {
      ProjectId,
      DatasetId,
      TableId,
      TableFriendlyName,
      TableType,
      ColumnName,
      ColumnType,
      ColumnMode,
      ColumnDescription,
      /** A JSON-serialized instance of {@link Metadata}. */
      ColumnMetadata
    }
  }
}
