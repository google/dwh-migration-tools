/*
 * Copyright 2022 Google LLC
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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 *
 * @author swapnil
 */
public interface HiveMetadataDumpFormat {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .disable(SerializationFeature.INDENT_OUTPUT);

    public static final String FORMAT_NAME = "hiveql.dump.zip";

    interface SchemataFormat {

        String ZIP_ENTRY_NAME = "schemata.csv";

        enum Header {
            SchemaName
        }
    }

    interface DatabasesFormat {

        String ZIP_ENTRY_NAME = "databases.csv";

        enum Header {
            Name,
            Description,
            Owner,
            OwnerType,
            Location,
        }
    }

    public static interface TablesJsonTaskFormat {

        public static final String ZIP_ENTRY_NAME = "tables.jsonl";

        @JsonIgnoreProperties(ignoreUnknown = true)
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public static class TableMetadata {

            @CheckForNull
            public static HiveMetadataDumpFormat.TablesJsonTaskFormat.TableMetadata fromJson(@CheckForNull String text) throws IOException {
                if (text == null)
                    return null;
                if (text.isEmpty())
                    return null;
                return MAPPER.readValue(text, HiveMetadataDumpFormat.TablesJsonTaskFormat.TableMetadata.class);
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            @JsonInclude(JsonInclude.Include.NON_ABSENT)
            public static class FieldMetadata {
                public String name;
                public String type;
                public String comment;
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            @JsonInclude(JsonInclude.Include.NON_ABSENT)
            public static class PartitionKeyMetadata {
                public String name;
                public String type;
                public String comment;
            }

            @JsonIgnoreProperties(ignoreUnknown = true)
            @JsonInclude(JsonInclude.Include.NON_ABSENT)
            public static class PartitionMetadata {
                public String name;
                public String location;
                public Integer createTime;
                public Integer lastAccessTime;
                public Integer lastDdlTime;
                public Long totalSize;
                public Long rawSize;
                public Long rowsCount;
                public Integer filesCount;
                public Boolean isCompressed;
            }

            @CheckForNull
            public String schemaName;
            @CheckForNull
            public String name;
            @CheckForNull
            public String type;
            @CheckForNull
            public Integer createTime;
            @CheckForNull
            public Integer lastAccessTime;
            @CheckForNull
            public String owner;
            @CheckForNull
            public String viewText;
            @CheckForNull
            public String location;
            @CheckForNull
            public Integer lastDdlTime;
            @CheckForNull
            public Long totalSize;
            @CheckForNull
            public Long rawSize;
            @CheckForNull
            public Long rowsCount;
            @CheckForNull
            public Integer filesCount;
            @CheckForNull
            public Integer retention;
            @CheckForNull
            public Integer bucketsCount;
            @CheckForNull
            public Boolean isCompressed;

            @CheckForNull
            public List<FieldMetadata> fields;
            @CheckForNull
            public List<PartitionKeyMetadata> partitionKeys;
            @CheckForNull
            public List<PartitionMetadata> partitions;
        }
    }

    @Deprecated // Use TablesJsonTaskFormat
    interface TablesFormat {

        String ZIP_ENTRY_NAME = "tables.csv";

        enum Header {
            TableSchema,
            TableName
        }
    }

    @Deprecated // Use TablesJsonTaskFormat
    interface ColumnsFormat {

        String ZIP_ENTRY_NAME = "columns.csv";

        enum Header {
            TableSchema,
            TableName,
            OrdinalPosition,
            ColumnName,
            DataType,
            IsPartitionKey,
            Comment
        }
    }

    interface FunctionsFormat {

        String ZIP_ENTRY_NAME = "functions.csv";

        enum Header {
            FunctionSchema,
            FunctionName,
            FunctionType,
            ClassName,
            OwnerName,
            OwnerType,
            CreateTime
        }
    }
}
