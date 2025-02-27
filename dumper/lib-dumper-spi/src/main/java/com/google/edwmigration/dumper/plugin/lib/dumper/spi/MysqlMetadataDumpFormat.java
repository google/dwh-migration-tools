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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

/** @author shevek */
public interface MysqlMetadataDumpFormat {

  String FORMAT_NAME = "mysql.dump.zip";

  interface SchemataFormat {

    String ZIP_ENTRY_NAME = "schemata.csv";

    enum Header {
      CATALOG_NAME,
      SCHEMA_NAME,
      DEFAULT_CHARACTER_SET_NAME,
      DEFAULT_COLLATION_NAME,
      SQL_PATH,
      DEFAULT_ENCRYPTION,
    }
  }

  interface TablesFormat {

    String ZIP_ENTRY_NAME = "tables.csv";

    enum Header {
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      TABLE_TYPE,
      ENGINE,
      VERSION,
      ROW_FORMAT,
      TABLE_ROWS,
      AVG_ROW_LENGTH,
      DATA_LENGTH,
      MAX_DATA_LENGTH,
      INDEX_LENGTH,
      DATA_FREE,
      AUTO_INCREMENT,
      CREATE_TIME,
      UPDATE_TIME,
      CHECK_TIME,
      TABLE_COLLATION,
      CHECKSUM,
      CREATE_OPTIONS,
      TABLE_COMMENT,
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
      NUMERIC_SCALE,
      DATETIME_PRECISION,
      CHARACTER_SET_NAME,
      COLLATION_NAME,
      COLUMN_TYPE,
      COLUMN_KEY,
      EXTRA,
      PRIVILEGES,
      COLUMN_COMMENT,
      GENERATION_EXPRESSION,
      SRS_ID,
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
      IS_UPDATABLE,
      DEFINER,
      SECURITY_TYPE,
      CHARACTER_SET_CLIENT,
      COLLATION_CONNECTION,
    }
  }

  interface FunctionsFormat {

    String ZIP_ENTRY_NAME = "functions.csv";

    enum Header {
      SPECIFIC_NAME,
      ROUTINE_CATALOG,
      ROUTINE_SCHEMA,
      ROUTINE_NAME,
      ROUTINE_TYPE,
      DATA_TYPE,
      CHARACTER_MAXIMUM_LENGTH,
      CHARACTER_OCTET_LENGTH,
      NUMERIC_PRECISION,
      NUMERIC_SCALE,
      DATETIME_PRECISION,
      CHARACTER_SET_NAME,
      COLLATION_NAME,
      DTD_IDENTIFIER,
      ROUTINE_BODY,
      ROUTINE_DEFINITION,
      EXTERNAL_NAME,
      EXTERNAL_LANGUAGE,
      PARAMETER_STYLE,
      IS_DETERMINISTIC,
      SQL_DATA_ACCESS,
      SQL_PATH,
      SECURITY_TYPE,
      CREATED,
      LAST_ALTERED,
      SQL_MODE,
      ROUTINE_COMMENT,
      DEFINER,
      CHARACTER_SET_CLIENT,
      COLLATION_CONNECTION,
      DATABASE_COLLATION,
    }
  }
}
