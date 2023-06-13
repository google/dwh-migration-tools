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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

/** @author dave */
public interface SnowflakeMetadataDumpFormat {

  public static final String FORMAT_NAME = "snowflake.dump.zip";

  interface DatabasesFormat {

    String IS_ZIP_ENTRY_NAME = "databases.csv";
    String AU_ZIP_ENTRY_NAME = "databases-au.csv";

    enum Header {
      DatabaseName,
      DatabaseOwner
    }
  }

  interface SchemataFormat {

    String IS_ZIP_ENTRY_NAME = "schemata.csv";
    String AU_ZIP_ENTRY_NAME = "schemata-au.csv";

    enum Header {
      CatalogName,
      SchemaName
    }
  }

  interface TablesFormat {

    String IS_ZIP_ENTRY_NAME = "tables.csv";
    String AU_ZIP_ENTRY_NAME = "tables-au.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      TableType,
      RowCount,
      Bytes,
      ClusteringKey
    }
  }

  interface ColumnsFormat {

    String IS_ZIP_ENTRY_NAME = "columns.csv";
    String AU_ZIP_ENTRY_NAME = "columns-au.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      OrdinalPosition,
      ColumnName,
      DataType
    }
  }

  interface ViewsFormat {

    String IS_ZIP_ENTRY_NAME = "views.csv";
    String AU_ZIP_ENTRY_NAME = "views-au.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      ViewDefinition
    }
  }

  interface FunctionsFormat {

    String IS_ZIP_ENTRY_NAME = "functions.csv";
    String AU_ZIP_ENTRY_NAME = "functions-au.csv";

    enum Header {
      FunctionSchema,
      FunctionName,
      DataType,
      ArgumentSignature
    }
  }

  interface WarehouseEventsHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_events-au.csv";

    enum Header {
      Timestamp,
      WarehouseId,
      WarehouseName,
      ClusterNumber,
      EventName,
      EventReason,
      EventState,
      UserName,
      RoleName,
      QueryId
    }
  }
}
