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

/** Output archive contract of the Databricks metadata connector. */
public interface DatabricksMetadataDumpFormat {

  String FORMAT_NAME = "databricks.dump.zip";

  /**
   * The {@code TableType} of a {@code tables-hms.csv} row and the {@code ConstraintType} of a
   * {@code table_constraints.csv} row of a table whose details couldn't be read.
   */
  String ERROR_TYPE = "ERROR";

  interface CatalogsFormat {

    String ZIP_ENTRY_NAME = "catalogs.csv";
    String HMS_ZIP_ENTRY_NAME = "catalogs-hms.csv";

    /**
     * Columns of {@code catalogs.csv} and {@code catalogs-hms.csv}. For Unity Catalog, the {@code
     * name} and {@code owner} of the REST {@code catalogs.list} catalogs.
     */
    enum Header {
      CatalogName,
      CatalogOwner
    }
  }

  interface SchemataFormat {

    String ZIP_ENTRY_NAME = "schemata.csv";
    String HMS_ZIP_ENTRY_NAME = "schemata-hms.csv";

    /**
     * Columns of {@code schemata.csv} and {@code schemata-hms.csv}. For Unity Catalog, the catalog
     * and {@code name} of the REST {@code schemas.list} schemas.
     */
    enum Header {
      CatalogName,
      SchemaName
    }
  }

  interface TablesFormat {

    String ZIP_ENTRY_NAME = "tables.csv";
    String HMS_ZIP_ENTRY_NAME = "tables-hms.csv";

    /**
     * Columns of {@code tables.csv} and {@code tables-hms.csv}.
     *
     * <ul>
     *   <li>For Unity Catalog, the {@code catalog_name}, {@code schema_name}, {@code name}, {@code
     *       table_type}, {@code data_source_format}, {@code storage_location}, {@code
     *       securable_kind} (such as {@code TABLE_DELTA} or {@code TABLE_MATERIALIZED_VIEW}),
     *       {@code pipeline_id} (of a materialized view or streaming table) and {@code
     *       properties["spark.sql.statistics.totalSize"]} (set by {@code ANALYZE TABLE} or by the
     *       automatic statistics of predictive optimization) of the REST {@code tables.list}
     *       tables. The {@code StoragePath}, {@code PipelineId} and {@code SizeInBytes} of a table
     *       with only the BROWSE privilege are empty, and the task summary counts these tables.
     *   <li>For {@code hive_metastore}, {@code SecurableKind} and {@code PipelineId} are empty, and
     *       {@code SizeInBytes} is the {@code sizeInBytes} of {@code DESCRIBE DETAIL} for Delta
     *       tables or the {@code statistics.size_in_bytes} of {@code DESCRIBE TABLE EXTENDED ... AS
     *       JSON} when table statistics have been computed. A table whose {@code DESCRIBE TABLE}
     *       fails has the {@code TableType} {@code ERROR} and empty other details; if {@code
     *       DESCRIBE TABLE} fails for every table, the task itself fails.
     * </ul>
     */
    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      TableType,
      DataSourceFormat,
      StoragePath,
      SecurableKind,
      PipelineId,
      SizeInBytes
    }
  }

  interface ColumnsFormat {

    String ZIP_ENTRY_NAME = "columns.csv";
    String HMS_ZIP_ENTRY_NAME = "columns-hms.csv";

    /**
     * Columns of {@code columns.csv} and {@code columns-hms.csv}.
     *
     * <ul>
     *   <li>For Unity Catalog, the {@code columns} of the REST {@code tables.list} tables: {@code
     *       OrdinalPosition} is the 0-based {@code position}, {@code DataType} is the {@code
     *       type_name} (such as LONG for a bigint column), {@code FullDataType} is the {@code
     *       type_text}, {@code NumericPrecision} and {@code NumericScale} are set for DECIMAL
     *       columns only, {@code CharacterMaximumLength} for CHAR and VARCHAR columns only, {@code
     *       DatetimePrecision} is empty, {@code TypeJson} is the REST {@code type_json}, and {@code
     *       ColumnDefault}, {@code IsIdentity}, {@code IdentityGeneration}, {@code IdentityStart},
     *       {@code IdentityIncrement}, {@code IsGenerated} and {@code GenerationExpression} come
     *       from {@code type_json.metadata}.
     *   <li>For {@code hive_metastore}, the columns of the {@code DESCRIBE TABLE ... AS JSON}
     *       output: {@code DataType} is the type name in upper case (such as BIGINT), {@code
     *       FullDataType} is the SQL type string, and {@code TypeJson} is the column JSON object,
     *       such as {@code {"name":"id","type":{"name":"bigint"},"nullable":false}}.
     * </ul>
     */
    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      OrdinalPosition,
      ColumnName,
      DataType,
      FullDataType,
      IsNullable,
      ColumnDefault,
      CharacterMaximumLength,
      NumericPrecision,
      NumericScale,
      DatetimePrecision,
      Comment,
      PartitionIndex,
      IsIdentity,
      IdentityGeneration,
      IdentityStart,
      IdentityIncrement,
      IsGenerated,
      GenerationExpression,
      TypeJson
    }
  }

  interface ViewsFormat {

    String ZIP_ENTRY_NAME = "views.csv";
    String HMS_ZIP_ENTRY_NAME = "views-hms.csv";

    /**
     * Columns of {@code views.csv} and {@code views-hms.csv}. For Unity Catalog, the REST {@code
     * tables.list} tables that have a {@code view_definition}.
     */
    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      ViewDefinition
    }
  }

  interface TableConstraintsFormat {

    String ZIP_ENTRY_NAME = "table_constraints.csv";
    String HMS_ZIP_ENTRY_NAME = "table_constraints-hms.csv";

    /**
     * Columns of {@code table_constraints.csv} and {@code table_constraints-hms.csv}.
     *
     * <ul>
     *   <li>For Unity Catalog, a row for each PRIMARY KEY, FOREIGN KEY and UNIQUE constraint of the
     *       {@code table_constraints} of the REST {@code tables.get} tables, and for each CHECK
     *       constraint of their {@code properties}. A table whose {@code tables.get} call fails has
     *       one row with its names and the {@code ConstraintType} {@code ERROR}, so that it differs
     *       from a table without constraints; if {@code tables.get} fails for every table, the task
     *       itself fails.
     *   <li>For {@code hive_metastore}, which has no key constraints, a row for each CHECK
     *       constraint of the {@code table_properties} of the {@code DESCRIBE TABLE EXTENDED ... AS
     *       JSON} output. A table whose {@code DESCRIBE TABLE} fails has a row with the {@code
     *       TableType} {@code ERROR} in {@code tables-hms.csv} only.
     * </ul>
     *
     * <p>{@code ConstraintColumns} and {@code ReferencedColumns} are JSON arrays of column names,
     * the referenced table and columns are set for foreign keys only, and {@code Rely} is {@code
     * true} or {@code false} for key constraints. Delta stores a CHECK constraint as the table
     * property {@code delta.constraints.<name>}, so {@code CheckClause} is the value of that
     * property, such as {@code id > 0}, and the CHECK constraints of a table are in name order.
     */
    enum Header {
      ConstraintCatalog,
      ConstraintSchema,
      ConstraintName,
      TableCatalog,
      TableSchema,
      TableName,
      ConstraintType,
      ConstraintColumns,
      ReferencedTableCatalog,
      ReferencedTableSchema,
      ReferencedTableName,
      ReferencedColumns,
      Rely,
      CheckClause
    }
  }

  interface ColumnMasksFormat {

    String ZIP_ENTRY_NAME = "column_masks.csv";

    /**
     * Columns of {@code column_masks.csv}: the {@code mask} of the columns of the REST {@code
     * tables.list} tables. {@code UsingColumns} are separated by commas.
     */
    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      ColumnName,
      MaskName,
      UsingColumns
    }
  }

  interface RowFiltersFormat {

    String ZIP_ENTRY_NAME = "row_filters.csv";

    /**
     * Columns of {@code row_filters.csv}: the {@code row_filter} of the REST {@code tables.list}
     * tables. {@code TargetColumns} are separated by a comma and a space.
     */
    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      FilterName,
      TargetColumns
    }
  }
}
