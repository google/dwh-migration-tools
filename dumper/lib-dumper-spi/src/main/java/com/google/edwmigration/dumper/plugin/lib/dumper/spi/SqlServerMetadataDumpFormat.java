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

/**
 * @author swapnil
 */
public interface SqlServerMetadataDumpFormat {

  public static final String FORMAT_NAME = "sqlserver.dump.zip";

  interface SchemataFormat {

    String ZIP_ENTRY_NAME = "schemata.csv";

    enum Header {
      CATALOG_NAME,
      SCHEMA_NAME,
      SCHEMA_OWNER,
      DEFAULT_CHARACTER_SET_CATALOG,
      DEFAULT_CHARACTER_SET_SCHEMA,
      DEFAULT_CHARACTER_SET_NAME,
    }
  }

  interface TablesFormat {

    String ZIP_ENTRY_NAME = "tables.csv";

    enum Header {
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      TABLE_TYPE
    }
  }

  interface ColumnsFormat {

    String ZIP_ENTRY_NAME = "columns.csv";

    enum Header {
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      COLUMN_NAME,
      ORDINAL_POSITION,
      COLUMN_DEFAULT,
      IS_NULLABLE,
      DATA_TYPE,
      CHARACTER_MAXIMUM_LENGTH,
      CHARACTER_OCTET_LENGTH,
      NUMERIC_PRECISION,
      NUMERIC_PRECISION_RADIX,
      NUMERIC_SCALE,
      DATETIME_PRECISION,
      CHARACTER_SET_CATALOG,
      CHARACTER_SET_SCHEMA,
      CHARACTER_SET_NAME,
      COLLATION_CATALOG,
      COLLATION_SCHEMA,
      COLLATION_NAME,
      DOMAIN_CATALOG,
      DOMAIN_SCHEMA,
      DOMAIN_NAME
    }
  }

  interface ViewsFormat {

    String ZIP_ENTRY_NAME = "views.csv";

    enum Header {
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      VIEW_DEFINITION,
      CHECK_OPTION,
      IS_UPDATABLE
    }
  }

  interface FunctionsFormat {

    String ZIP_ENTRY_NAME = "functions.csv";

    enum Header {
      SPECIFIC_CATALOG,
      SPECIFIC_SCHEMA,
      SPECIFIC_NAME,
      ROUTINE_CATALOG,
      ROUTINE_SCHEMA,
      ROUTINE_NAME,
      ROUTINE_TYPE,
      MODULE_CATALOG,
      MODULE_SCHEMA,
      MODULE_NAME,
      UDT_CATALOG,
      UDT_SCHEMA,
      UDT_NAME,
      DATA_TYPE,
      CHARACTER_MAXIMUM_LENGTH,
      CHARACTER_OCTET_LENGTH,
      COLLATION_CATALOG,
      COLLATION_SCHEMA,
      COLLATION_NAME,
      CHARACTER_SET_CATALOG,
      CHARACTER_SET_SCHEMA,
      CHARACTER_SET_NAME,
      NUMERIC_PRECISION,
      NUMERIC_PRECISION_RADIX,
      NUMERIC_SCALE,
      DATETIME_PRECISION,
      INTERVAL_TYPE,
      INTERVAL_PRECISION,
      TYPE_UDT_CATALOG,
      TYPE_UDT_SCHEMA,
      TYPE_UDT_NAME,
      SCOPE_CATALOG,
      SCOPE_SCHEMA,
      SCOPE_NAME,
      MAXIMUM_CARDINALITY,
      DTD_IDENTIFIER,
      ROUTINE_BODY,
      ROUTINE_DEFINITION,
      EXTERNAL_NAME,
      EXTERNAL_LANGUAGE,
      PARAMETER_STYLE,
      IS_DETERMINISTIC,
      SQL_DATA_ACCESS,
      IS_NULL_CALL,
      SQL_PATH,
      SCHEMA_LEVEL_ROUTINE,
      MAX_DYNAMIC_RESULT_SETS,
      IS_USER_DEFINED_CAST,
      IS_IMPLICITLY_INVOCABLE,
      CREATED,
      LAST_ALTERED
    }
  }
}
