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

/**
 *
 * @author shevek
 */
public interface RedshiftMetadataDumpFormat extends PostgresqlMetadataDumpFormat {

    public static final String FORMAT_NAME = "redshift.dump.zip";

    /** This appears to be a superset of SVV_COLUMNS_EXTERNAL. */
    public static interface SvvColumnsFormat {

        public static final String ZIP_ENTRY_NAME = "svv_columns.csv";

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
            domain_name,
            remarks
        }
    }

    public static interface InformationSchemaColumns {

        public static final String ZIP_ENTRY_NAME_SYSTEM = PostgresqlMetadataDumpFormat.InformationSchemaColumns.ZIP_ENTRY_NAME_SYSTEM;
        public static final String ZIP_ENTRY_NAME = PostgresqlMetadataDumpFormat.InformationSchemaColumns.ZIP_ENTRY_NAME;

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
            is_self_referencing
        }
    }

    @Deprecated // Does not give information about column order. TODO: Use SVV_TABLE_INFO.
    public static interface PgTableDef {

        public static final String ZIP_ENTRY_NAME_SYSTEM = "pg_table_def_generic.csv";
        public static final String ZIP_ENTRY_NAME = "pg_table_def_private.csv";

        public static enum Header {

            schemaname,
            tablename,
            column,
            type,
            encoding,
            distkey,
            sortkey,
            notnull
            /* Trailing fields are optional; as long as we match on the important ones. */
            // column_set_using,   // Used not to be present, but is in Elon's dump.
            // is_identity;
        }
    }
}
