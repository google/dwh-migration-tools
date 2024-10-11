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
 * @author zzwang
 */
public interface GreenplumMetadataDumpFormat {

  public static final String FORMAT_NAME = "greenplum.dump.zip";

  public static interface InformationSchemaColumns {

    public static final String ZIP_ENTRY_NAME_SYSTEM = "is_columns_generic.csv";
    public static final String ZIP_ENTRY_NAME = "is_columns_private.csv";

    public static enum Header {
      table_catalog,
      table_schema,
      table_name,
      column_name,
      ordinal_position,
      column_default,
      is_nullable,
      data_type,
      character_maximum_length,
      character_octet_length,
      numeric_precision,
      numeric_precision_radix,
      numeric_scale,
      datetime_precision,
      interval_type,
      interval_precision,
      character_set_catalog,
      character_set_schema,
      character_set_name,
      collation_catalog,
      collation_schema,
      collation_name,
      domain_catalog,
      domain_schema,
      domain_name,
      udt_catalog,
      udt_schema,
      udt_name,
      scope_catalog,
      scope_schema,
      scope_name,
      maximum_cardinality,
      dtd_identifier,
      is_self_referencing,
      is_identity,
      identity_generation,
      identity_start,
      identity_increment,
      identity_maximum,
      identity_minimum,
      identity_cycle,
      is_generated,
      generation_expression,
      is_updatable
    }
  }

  public static interface PgViews {

    public static final String ZIP_ENTRY_NAME_SYSTEM = "pg_views_generic.csv";
    public static final String ZIP_ENTRY_NAME = "pg_views_private.csv";

    public static enum Header {
      schemaname,
      viewname,
      viewowner,
      definition
    }
  }

  // functions, functions-11, aggregates, aggregates-11
  public static interface PgFunctions {

    public static final String ZIP_ENTRY_NAME_SYSTEM = "pg_functions_generic.csv";
    public static final String ZIP_ENTRY_NAME = "pg_functions_private.csv";

    public static enum Header {
      Schema,
      Name,
      ResultType,
      ArgumentTypes,
      /** Not in aggregates or aggregates-11. */
      Kind,
      /** Not in functions. */
      Description;
    }
  }

  public static interface PgUser {

    public static final String ZIP_ENTRY_NAME = "pg_user.csv";

    public static enum Header {
      username,
      userid,
      usecreatedb,
      usesuper,
      userepl,
      usebypassrls,
      passwd,
      valuntil,
      useconfig
    }
  }
}
