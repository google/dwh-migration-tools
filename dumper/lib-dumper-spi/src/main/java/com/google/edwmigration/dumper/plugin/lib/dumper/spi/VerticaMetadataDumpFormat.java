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

/** */
public interface VerticaMetadataDumpFormat {

  public static final String FORMAT_NAME = "vertica.dump.zip";

  interface AllTablesFormat {

    String ZIP_ENTRY_NAME = "all_tables.csv";

    enum Header {}
  }

  interface ColumnsFormat {

    String ZIP_ENTRY_NAME = "columns.csv";

    enum Header {
      table_id,
      table_schema,
      table_name,
      is_system_table,
      column_id,
      column_name,
      data_type,
      data_type_id,
      data_type_length,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      datetime_precision,
      interval_precision,
      ordinal_position,
      is_nullable,
      column_default,
      column_set_using,
      is_identity,
    }
  }

  interface ConstraintColumnsFormat {

    String ZIP_ENTRY_NAME = "constraint_columns.csv";

    enum Header {}
  }

  interface DatabasesFormat {

    String ZIP_ENTRY_NAME = "databases.csv";

    enum Header {}
  }

  interface KeywordsFormat {

    String ZIP_ENTRY_NAME = "keywords.csv";

    enum Header {}
  }

  interface PrimaryKeysFormat {

    String ZIP_ENTRY_NAME = "primary_keys.csv";

    enum Header {
      constraint_id,
      constraint_name,
      column_name,
      ordinal_position,
      table_name,
      constraint_type,
      is_enabled,
      table_schema
    }
  }

  interface SchemataFormat {

    String ZIP_ENTRY_NAME = "schemata.csv";

    enum Header {
      schema_id,
      schema_name,
      schema_owner_id,
      schema_owner,
      system_schema_creator,
      create_time,
      is_system_schema,
    }
  }

  interface TableConstraintsFormat {

    String ZIP_ENTRY_NAME = "table_constraints.csv";

    enum Header {}
  }

  // https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/SystemTables/CATALOG/TABLES.htm
  interface TablesFormat {

    String ZIP_ENTRY_NAME = "tables.csv";

    enum Header {
      table_schema_id,
      table_schema,
      table_id,
      table_name,
      owner_id,
      owner_name,
      is_temp_table,
      is_system_table,
      force_outer,
      is_flextable,
      is_shared,
      table_has_aggregate_projection,
      system_table_creator,
      partition_expression,
      create_time,
      table_definition,
      recover_priority,
      storage_mode,
      partition_group_expression,
      active_partition_count,
    }
  }

  interface TypesFormat {

    String ZIP_ENTRY_NAME = "types.csv";

    enum Header {}
  }

  interface UserFunctionParametersFormat {

    String ZIP_ENTRY_NAME = "user_function_parameters.csv";

    enum Header {}
  }

  interface UserFunctionsFormat {

    String ZIP_ENTRY_NAME = "user_functions.csv";

    enum Header {}
  }

  interface UserProceduresFormat {

    String ZIP_ENTRY_NAME = "user_procedures.csv";

    enum Header {}
  }

  interface UsersFormat {

    String ZIP_ENTRY_NAME = "users.csv";

    enum Header {}
  }

  interface ViewColumnsFormat {

    String ZIP_ENTRY_NAME = "view_columns.csv";

    enum Header {}
  }

  interface ViewTablesFormat {

    String ZIP_ENTRY_NAME = "view_tables.csv";

    enum Header {}
  }

  // https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/SystemTables/CATALOG/VIEWS.htm
  interface ViewsFormat {

    String ZIP_ENTRY_NAME = "views.csv";

    enum Header {
      table_schema_id,
      table_schema,
      table_id,
      table_name,
      owner_id,
      owner_name,
      view_definition,
      is_system_view,
      system_view_creator,
      create_time,
      is_local_temp_view,
      inherit_privileges,
    }
  }
}
