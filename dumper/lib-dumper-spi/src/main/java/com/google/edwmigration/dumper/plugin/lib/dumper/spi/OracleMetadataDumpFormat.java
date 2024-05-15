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
 * Oracle Database Reference : Part II Static Data Dictionary Views
 * https://github.com/CompilerWorks/reference/blob/master/oracle/database-reference.pdf
 *
 * <p>ALL_* -- all info accesible to current user, including grants DBA_* -- need SELECT ANY TABLE
 * privilege, DBA ROLE USER_* -- all schema _of_ current user We should prefer DBA_* , but have a
 * fallback of ALL_* and USER_* The names here are without prefix
 */
public interface OracleMetadataDumpFormat {

  public static final String FORMAT_NAME = "oracle.dump.zip";

  interface Arguments {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Arguments.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Arguments.csv";

    enum Header {
      OWNER,
      OBJECT_NAME,
      PACKAGE_NAME,
      OBJECT_ID,
      OVERLOAD,
      SUBPROGRAM_ID,
      ARGUMENT_NAME,
      POSITION,
      SEQUENCE,
      DATA_LEVEL,
      DATA_TYPE,
      DEFAULTED,
      DEFAULT_VALUE,
      DEFAULT_LENGTH,
      IN_OUT,
      DATA_LENGTH,
      DATA_PRECISION,
      DATA_SCALE,
      RADIX,
      CHARACTER_SET_NAME,
      TYPE_OWNER,
      TYPE_NAME,
      TYPE_SUBNAME,
      TYPE_LINK,
      PLS_TYPE,
      CHAR_LENGTH,
      CHAR_USED,
      ORIGIN_CON_ID
    }
  }

  interface Catalog {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Catalog.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Catalog.csv";

    enum Header {
      OWNER,
      TABLE_NAME,
      TABLE_TYPE
    }
  }

  interface Constraints {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Constraints.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Constraints.csv";

    enum Header {
      OWNER,
      CONSTRAINT_NAME,
      CONSTRAINT_TYPE,
      TABLE_NAME,
      SEARCH_CONDITION,
      SEARCH_CONDITION_VC,
      R_OWNER,
      R_CONSTRAINT_NAME,
      DELETE_RULE,
      STATUS,
      DEFERRABLE,
      DEFERRED,
      VALIDATED,
      GENERATED,
      BAD,
      RELY,
      LAST_CHANGE,
      INDEX_OWNER,
      INDEX_NAME,
      INVALID,
      VIEW_RELATED,
      ORIGIN_CON_ID
    }
  }

  interface Functions {

    final String ZIP_ENTRY_NAME = "Functions.csv";

    enum Header {
      STATEMENT
    }
  }

  interface Indexes {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Indexes.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Indexes.csv";

    enum Header {
      OWNER,
      INDEX_NAME,
      INDEX_TYPE,
      TABLE_OWNER,
      TABLE_NAME,
      TABLE_TYPE,
      UNIQUENESS,
      COMPRESSION,
      PREFIX_LENGTH,
      TABLESPACE_NAME,
      INI_TRANS,
      MAX_TRANS,
      INITIAL_EXTENT,
      NEXT_EXTENT,
      MIN_EXTENTS,
      MAX_EXTENTS,
      PCT_INCREASE,
      PCT_THRESHOLD,
      INCLUDE_COLUMN,
      FREELISTS,
      FREELIST_GROUPS,
      PCT_FREE,
      LOGGING,
      BLEVEL,
      LEAF_BLOCKS,
      DISTINCT_KEYS,
      AVG_LEAF_BLOCKS_PER_KEY,
      AVG_DATA_BLOCKS_PER_KEY,
      CLUSTERING_FACTOR,
      STATUS,
      NUM_ROWS,
      SAMPLE_SIZE,
      LAST_ANALYZED,
      DEGREE,
      INSTANCES,
      PARTITIONED,
      TEMPORARY,
      GENERATED,
      SECONDARY,
      BUFFER_POOL,
      FLASH_CACHE,
      CELL_FLASH_CACHE,
      USER_STATS,
      DURATION,
      PCT_DIRECT_ACCESS,
      ITYP_OWNER,
      ITYP_NAME,
      PARAMETERS,
      GLOBAL_STATS,
      DOMIDX_STATUS,
      DOMIDX_OPSTATUS,
      FUNCIDX_STATUS,
      JOIN_INDEX,
      IOT_REDUNDANT_PKEY_ELIM,
      DROPPED,
      VISIBILITY,
      DOMIDX_MANAGEMENT,
      SEGMENT_CREATED,
      ORPHANED_ENTRIES,
      INDEXING
    }
  }

  interface MViews {

    final String ZIP_ENTRY_NAME_DBA = "DBA.MViews.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.MViews.csv";

    enum Header {
      OWNER,
      MVIEW_NAME,
      CONTAINER_NAME,
      QUERY,
      QUERY_LEN,
      UPDATABLE,
      UPDATE_LOG,
      MASTER_ROLLBACK_SEG,
      MASTER_LINK,
      REWRITE_ENABLED,
      REWRITE_CAPABILITY,
      REFRESH_MODE,
      REFRESH_METHOD,
      BUILD_MODE,
      FAST_REFRESHABLE,
      LAST_REFRESH_TYPE,
      LAST_REFRESH_DATE,
      LAST_REFRESH_END_TIME,
      STALENESS,
      AFTER_FAST_REFRESH,
      UNKNOWN_PREBUILT,
      UNKNOWN_PLSQL_FUNC,
      UNKNOWN_EXTERNAL_TABLE,
      UNKNOWN_CONSIDER_FRESH,
      UNKNOWN_IMPORT,
      UNKNOWN_TRUSTED_FD,
      COMPILE_STATE,
      USE_NO_INDEX,
      STALE_SINCE,
      NUM_PCT_TABLES,
      NUM_FRESH_PCT_REGIONS,
      NUM_STALE_PCT_REGIONS,
      SEGMENT_CREATED,
      EVALUATION_EDITION,
      UNUSABLE_BEFORE,
      UNUSABLE_BEGINNING,
      DEFAULT_COLLATION,
      ON_QUERY_COMPUTATION
    }
  }

  interface Operators {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Operators.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Operators.csv";

    enum Header {
      OWNER,
      OPERATOR_NAME,
      NUMBER_OF_BINDS
    }
  }

  interface Part_key_columns {

    final String ZIP_ENTRY_NAME_DBA = "DBA.PartKeyColumns.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.PartKeyColumns.csv";

    enum Header {
      OWNER,
      NAME,
      OBJECT_TYPE,
      COLUMN_NAME,
      COLUMN_POSITION,
      COLLATED_COLUMN_ID
    }
  }

  interface Plsql_Types {

    final String ZIP_ENTRY_NAME_DBA = "DBA.PlsqlTypes.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.PlsqlTypes.csv";

    enum Header {
      OWNER,
      TYPE_NAME,
      PACKAGE_NAME,
      TYPE_OID,
      TYPECODE,
      ATTRIBUTES,
      CONTAINS_PLSQL
    }
  }

  interface Procedures {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Procedures.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Procedures.csv";

    enum Header {
      OWNER,
      OBJECT_NAME,
      PROCEDURE_NAME,
      OBJECT_ID,
      SUBPROGRAM_ID,
      OVERLOAD,
      OBJECT_TYPE,
      AGGREGATE,
      PIPELINED,
      IMPLTYPEOWNER,
      IMPLTYPENAME,
      PARALLEL,
      INTERFACE,
      DETERMINISTIC,
      AUTHID,
      RESULT_CACHE,
      ORIGIN_CON_ID
    }
  }

  interface Tab_Columns {

    final String ZIP_ENTRY_NAME_DBA = "DBA.TabColumns.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.TabColumns.csv";

    enum Header {
      OWNER,
      TABLE_NAME,
      COLUMN_NAME,
      DATA_TYPE,
      DATA_TYPE_MOD,
      DATA_TYPE_OWNER,
      DATA_LENGTH,
      DATA_PRECISION,
      DATA_SCALE,
      NULLABLE,
      COLUMN_ID,
      DEFAULT_LENGTH,
      DATA_DEFAULT,
      NUM_DISTINCT,
      LOW_VALUE,
      HIGH_VALUE,
      DENSITY,
      NUM_NULLS,
      NUM_BUCKETS,
      LAST_ANALYZED,
      SAMPLE_SIZE,
      CHARACTER_SET_NAME,
      CHAR_COL_DECL_LENGTH,
      GLOBAL_STATS,
      USER_STATS,
      AVG_COL_LEN,
      CHAR_LENGTH,
      CHAR_USED,
      V80_FMT_IMAGE,
      DATA_UPGRADED,
      HISTOGRAM,
      DEFAULT_ON_NULL,
      IDENTITY_COLUMN,
      EVALUATION_EDITION,
      UNUSABLE_BEFORE,
      UNUSABLE_BEGINNING,
      COLLATION
    }
  }

  interface Tab_Partitions {

    final String ZIP_ENTRY_NAME_DBA = "DBA.TabPartitions.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.TabPartitions.csv";

    enum Header {
      TABLE_OWNER,
      TABLE_NAME,
      COMPOSITE,
      PARTITION_NAME,
      SUBPARTITION_COUNT,
      HIGH_VALUE,
      HIGH_VALUE_LENGTH,
      PARTITION_POSITION,
      TABLESPACE_NAME,
      PCT_FREE,
      PCT_USED,
      INI_TRANS,
      MAX_TRANS,
      INITIAL_EXTENT,
      NEXT_EXTENT,
      MIN_EXTENT,
      MAX_EXTENT,
      MAX_SIZE,
      PCT_INCREASE,
      FREELISTS,
      FREELIST_GROUPS,
      LOGGING,
      COMPRESSION,
      COMPRESS_FOR,
      NUM_ROWS,
      BLOCKS,
      EMPTY_BLOCKS,
      AVG_SPACE,
      CHAIN_CNT,
      AVG_ROW_LEN,
      SAMPLE_SIZE,
      LAST_ANALYZED,
      BUFFER_POOL,
      FLASH_CACHE,
      CELL_FLASH_CACHE,
      GLOBAL_STATS,
      USER_STATS,
      IS_NESTED,
      PARENT_TABLE_PARTITION,
      INTERVAL,
      SEGMENT_CREATED,
      INDEXING,
      READ_ONLY,
      INMEMORY,
      INMEMORY_PRIORITY,
      INMEMORY_DISTRIBUTE,
      INMEMORY_COMPRESSION,
      INMEMORY_DUPLICATE,
      CELLMEMORY,
      INMEMORY_SERVICE,
      INMEMORY_SERVICE_NAME
    }
  }

  interface Tables {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Tables.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Tables.csv";

    enum Header {
      OWNER,
      TABLE_NAME,
      TABLESPACE_NAME,
      CLUSTER_NAME,
      IOT_NAME,
      STATUS,
      PCT_FREE,
      PCT_USED,
      INI_TRANS,
      MAX_TRANS,
      INITIAL_EXTENT,
      NEXT_EXTENT,
      MIN_EXTENTS,
      MAX_EXTENTS,
      PCT_INCREASE,
      FREELISTS,
      FREELIST_GROUPS,
      LOGGING,
      BACKED_UP,
      NUM_ROWS,
      BLOCKS,
      EMPTY_BLOCKS,
      AVG_SPACE,
      CHAIN_CNT,
      AVG_ROW_LEN,
      AVG_SPACE_FREELIST_BLOCKS,
      NUM_FREELIST_BLOCKS,
      DEGREE,
      INSTANCES,
      CACHE,
      TABLE_LOCK,
      SAMPLE_SIZE,
      LAST_ANALYZED,
      PARTITIONED,
      IOT_TYPE,
      TEMPORARY,
      SECONDARY,
      NESTED,
      BUFFER_POOL,
      FLASH_CACHE,
      CELL_FLASH_CACHE,
      ROW_MOVEMENT,
      GLOBAL_STATS,
      USER_STATS,
      DURATION,
      SKIP_CORRUPT,
      MONITORING,
      CLUSTER_OWNER,
      DEPENDENCIES,
      COMPRESSION,
      COMPRESS_FOR,
      DROPPED,
      READ_ONLY,
      SEGMENT_CREATED,
      RESULT_CACHE,
      CLUSTERING,
      ACTIVITY_TRACKING,
      DML_TIMESTAMP,
      HAS_IDENTITY,
      CONTAINER_DATA,
      INMEMORY,
      INMEMORY_PRIORITY,
      INMEMORY_DISTRIBUTE,
      INMEMORY_COMPRESSION,
      INMEMORY_DUPLICATE,
      DEFAULT_COLLATION,
      DUPLICATED,
      SHARDED,
      EXTERNAL,
      CELLMEMORY,
      CONTAINERS_DEFAULT,
      CONTAINER_MAP,
      EXTENDED_DATA_LINK,
      EXTENDED_DATA_LINK_MAP,
      INMEMORY_SERVICE,
      INMEMORY_SERVICE_NAME,
      CONTAINER_MAP_OBJECT
    }
  }

  interface Types {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Types.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Types.csv";

    enum Header {
      OWNER,
      TYPE_NAME,
      TYPE_OID,
      TYPECODE,
      ATTRIBUTES,
      METHODS,
      PREDEFINED,
      INCOMPLETE,
      FINAL,
      INSTANTIABLE,
      SUPERTYPE_OWNER,
      SUPERTYPE_NAME,
      LOCAL_ATTRIBUTES,
      LOCAL_METHODS,
      TYPEID
    }
  }

  interface Views {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Views.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Views.csv";

    enum Header {
      OWNER,
      VIEW_NAME,
      TEXT_LENGTH,
      TEXT,
      TEXT_VC,
      TYPE_TEXT_LENGTH,
      TYPE_TEXT,
      OID_TEXT_LENGTH,
      OID_TEXT,
      VIEW_TYPE_OWNER,
      VIEW_TYPE,
      SUPERVIEW_NAME,
      EDITIONING_VIEW,
      READ_ONLY,
      CONTAINER_DATA,
      BEQUEATH,
      ORIGIN_CON_ID,
      DEFAULT_COLLATION,
      CONTAINERS_DEFAULT,
      CONTAINER_MAP,
      EXTENDED_DATA_LINK,
      EXTENDED_DATA_LINK_MAP
    }
  }

  interface V_Version {

    final String ZIP_ENTRY_NAME = "V_Version.csv";

    enum Header {
      BANNER,
      CON_ID,
    }
  }

  interface V_Parameter2 {

    final String ZIP_ENTRY_NAME = "V_Parameter2.csv";

    enum Header {
      NUM,
      NAME,
      TYPE,
      VALUE,
      DISPLAY_VALUE,
      ISDEFAULT,
      ISSES_MODIFIABLE,
      ISSYS_MODIFIABLE,
      ISPDB_MODIFIABLE,
      ISINSTANCE_MODIFIABLE,
      ISMODIFIED,
      ISADJUSTED,
      ISDEPRECATED,
      ISBASIC,
      DESCRIPTION,
      ORDINAL,
      UPDATE_COMMENT,
      CON_ID
    }
  }

  // Probably useless .. doesn't give the list of users.
  interface V_Pwfile_users {

    final String ZIP_ENTRY_NAME = "V_Pwfile_users.csv";

    enum Header {
      USERNAME,
      SYSDBA,
      SYSOPER,
      SYSASM,
      SYSBACKUP,
      SYSDG,
      SYSKM,
      ACCOUNT_STATUS,
      PASSWORD_PROFILE,
      LAST_LOGIN,
      LOCK_DATE,
      EXPIRY_DATE,
      EXTERNAL_NAME,
      AUTHENTICATION_TYPE,
      COMMON,
      CON_ID
    }
  }

  interface V_Option {

    final String ZIP_ENTRY_NAME = "V_Option.csv";

    enum Header {
      PARAMETER,
      VALUE,
      // Not present?
      CON_ID
    }
  }

  interface XmlTables {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Tables-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Tables-XML.csv";

    enum Header {
      OWNER,
      TABLE_NAME,
      XML
    }
  }

  interface XmlViews {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Views-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Views-XML.csv";

    enum Header {
      OWNER,
      VIEW_NAME,
      XML
    }
  }

  interface XmlIndexes {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Indexes-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Indexes-XML.csv";

    enum Header {
      OWNER,
      INDEX_NAME,
      XML
    }
  }

  interface XmlSequences {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Sequences-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Sequences-XML.csv";

    enum Header {
      OWNER,
      SEQUENCE_NAME,
      XML
    }
  }

  interface XmlFunctions {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Functions-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Functions-XML.csv";

    enum Header {
      OWNER,
      FUNCTION_NAME,
      XML
    }
  }

  interface XmlTypes {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Types-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Types-XML.csv";

    enum Header {
      OWNER,
      TYPE_NAME,
      XML
    }
  }

  interface XmlSynonyms {

    final String ZIP_ENTRY_NAME_DBA = "DBA.Synonyms-XML.csv";
    final String ZIP_ENTRY_NAME_ALL = "ALL.Synonyms-XML.csv";

    enum Header {
      OWNER,
      SYNONYM_NAME,
      XML
    }
  }
}
