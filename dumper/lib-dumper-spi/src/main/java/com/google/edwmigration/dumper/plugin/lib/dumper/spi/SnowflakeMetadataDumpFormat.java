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

  interface AutomaticClusteringHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "automatic_clustering_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      TABLE_NAME,
      CREDITS_USED,
      NUM_BYTES_RECLUSTERED,
      NUM_ROWS_RECLUSTERED
    }
  }

  interface CopyHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "copy_history-au.csv";

    enum Header {
      FILE_NAME,
      STAGE_LOCATION,
      LAST_LOAD_TIME,
      ROW_COUNT,
      ROW_PARSED,
      FILE_SIZE,
      FIRST_ERROR_MESSAGE,
      FIRST_ERROR_LINE_NUMBER,
      FIRST_ERROR_CHARACTER_POS,
      FIRST_ERROR_COLUMN_NAME,
      ERROR_COUNT,
      ERROR_LIMIT,
      STATUS,
      TABLE_CATALOG_NAME,
      TABLE_SCHEMA_NAME,
      TABLE_NAME,
      PIPE_CATALOG_NAME,
      PIPE_SCHEMA_NAME,
      PIPE_NAME,
      PIPE_RECEIVED_TIME
    }
  }

  interface DatabaseReplicationUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "database_replication_usage_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      DATABASE_NAME,
      CREDITS_USED,
      BYTES_TRANSFERRED
    }
  }

  interface LoginHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "login_history-au.csv";

    enum Header {
      READER_ACCOUNT_NAME,
      EVENT_ID,
      EVENT_TIMESTAMP,
      EVENT_TYPE,
      USER_NAME,
      CLIENT_IP,
      REPORTED_CLIENT_TYPE,
      REPORTED_CLIENT_VERSION,
      FIRST_AUTHENTICATION_FACTOR,
      SECOND_AUTHENTICATION_FACTOR,
      IS_SUCCESS,
      ERROR_CODE,
      ERROR_MESSAGE,
      RELATED_EVENT_ID,
      CONNECTION
    }
  }

  interface MeteringDailyHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "metering_daily_history-au.csv";

    enum Header {
      SERVICE_TYPE,
      ORGANIZATION_NAME,
      ACCOUNT_NAME,
      ACCOUNT_LOCATOR,
      USAGE_DATE,
      CREDITS_USED_COMPUTE,
      CREDITS_USED_CLOUD_SERVICES,
      CREDITS_USED,
      CREDITS_ADJUSTMENT_CLOUD_SERVICES,
      CREDITS_BILLED,
      REGION
    }
  }

  interface PipeUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "pipe_usage_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      PIPE_NAME,
      CREDITS_USED,
      BYTES_INSERTED,
      FILES_INSERTED
    }
  }

  interface QueryAccelerationHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "query_acceleration_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      CREDITS_USED,
      WAREHOUSE_NAME,
      NUM_FILES_SCANNED,
      NUM_BYTES_SCANNED
    }
  }

  interface QueryHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "query_history-au.csv";

    enum Header {
      READER_ACCOUNT_NAME,
      QUERY_ID,
      QUERY_TEXT,
      DATABASE_ID,
      DATABASE_NAME,
      SCHEMA_ID,
      SCHEMA_NAME,
      QUERY_TYPE,
      SESSION_ID,
      USER_NAME,
      ROLE_NAME,
      WAREHOUSE_ID,
      WAREHOUSE_NAME,
      WAREHOUSE_SIZE,
      WAREHOUSE_TYPE,
      CLUSTER_NUMBER,
      QUERY_TAG,
      EXECUTION_STATUS,
      ERROR_CODE,
      ERROR_MESSAGE,
      START_TIME,
      END_TIME,
      TOTAL_ELAPSED_TIME,
      BYTES_SCANNED,
      PERCENTAGE_SCANNED_FROM_CACHE,
      BYTES_WRITTEN,
      BYTES_WRITTEN_TO_RESULT,
      BYTES_READ_FROM_RESULT,
      ROWS_PRODUCED,
      ROWS_INSERTED,
      ROWS_UPDATED,
      ROWS_DELETED,
      ROWS_UNLOADED,
      BYTES_DELETED,
      PARTITIONS_SCANNED,
      PARTITIONS_TOTAL,
      BYTES_SPILLED_TO_LOCAL_STORAGE,
      BYTES_SPILLED_TO_REMOTE_STORAGE,
      BYTES_SENT_OVER_THE_NETWORK,
      COMPILATION_TIME,
      EXECUTION_TIME,
      QUEUED_PROVISIONING_TIME,
      QUEUED_REPAIR_TIME,
      QUEUED_OVERLOAD_TIME,
      TRANSACTION_BLOCKED_TIME,
      OUTBOUND_DATA_TRANSFER_CLOUD,
      OUTBOUND_DATA_TRANSFER_REGION,
      OUTBOUND_DATA_TRANSFER_BYTES,
      INBOUND_DATA_TRANSFER_CLOUD,
      INBOUND_DATA_TRANSFER_REGION,
      INBOUND_DATA_TRANSFER_BYTES,
      LIST_EXTERNAL_FILES_TIME,
      CREDITS_USED_CLOUD_SERVICES,
      RELEASE_VERSION,
      EXTERNAL_FUNCTION_TOTAL_INVOCATIONS,
      EXTERNAL_FUNCTION_TOTAL_SENT_ROWS,
      EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS,
      EXTERNAL_FUNCTION_TOTAL_SENT_BYTES,
      EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES,
      QUERY_LOAD_PERCENT,
      IS_CLIENT_GENERATED_STATEMENT,
      QUERY_ACCELERATION_BYTES_SCANNED,
      QUERY_ACCELERATION_PARTITIONS_SCANNED,
      QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR,
      CHILD_QUERIES_WAIT_TIME,
      OWNER,
      OWNER_ROLE_TYPE,
      TRANSACTION_ID,
      ROLE_TYPE
    }
  }

  interface ReplicationGroupUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "replication_group_usage_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      REPLICATION_GROUP_NAME,
      CREDITS_USED,
      BYTES_TRANSFERRED
    }
  }

  interface ServerlessTaskHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "serverless_task_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      TASK_NAME,
      CREDITS_USED
    }
  }

  interface TableStorageMetricsFormat {

    String AU_ZIP_ENTRY_NAME = "table_storage_metrics-au.csv";

    enum Header {
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      ID,
      CLONE_GROUP_ID,
      IS_TRANSIENT,
      ACTIVE_BYTES,
      TIME_TRAVEL_BYTES,
      FAILSAFE_BYTES,
      RETAINED_FOR_CLONE_BYTES,
      TABLE_CREATED,
      TABLE_DROPPED,
      TABLE_ENTERED_FAILSAFE,
      CATALOG_CREATED,
      CATALOG_DROPPED,
      SCHEMA_CREATED,
      SCHEMA_DROPPED,
      COMMENT
    }
  }

  interface TaskHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "task_history-au.csv";

    enum Header {
      QUERY_ID,
      NAME,
      DATABASE_NAME,
      SCHEMA_NAME,
      QUERY_TEXT,
      CONDITION_TEXT,
      STATE,
      ERROR_CODE,
      ERROR_MESSAGE,
      SCHEDULED_TIME,
      QUERY_START_TIME,
      NEXT_SCHEDULED_TIME,
      COMPLETED_TIME,
      ROOT_TASK_ID,
      GRAPH_VERSION,
      RUN_ID,
      RETURN_VALUE,
      SCHEDULED_FROM
    }
  }

  interface WarehouseLoadHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_load_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      WAREHOUSE_NAME,
      AVG_RUNNING,
      AVG_QUEUED_LOAD,
      AVG_QUEUED_PROVISIONING,
      AVG_BLOCKED
    }
  }

  interface WarehouseMeteringHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_metering_history-au.csv";

    enum Header {
      START_TIME,
      END_TIME,
      WAREHOUSE_NAME,
      CREDITS_USED,
      CREDITS_USED_COMPUTE,
      CREDITS_USED_CLOUD_SERVICES
    }
  }

  interface ShowWarehousesFormat {

    String AU_ZIP_ENTRY_NAME = "show_warehouses-au.csv";

    enum Header {
      name,
      state,
      type,
      size,
      min_cluster_count,
      max_cluster_count,
      started_clusters,
      running,
      queued,
      is_default,
      is_current,
      auto_suspend,
      auto_resume,
      available,
      provisioning,
      quiescing,
      other,
      created_on,
      resumed_on,
      updated_on,
      owner,
      comment,
      enable_query_acceleration,
      query_acceleration_max_scale_factor,
      resource_monitor,
      scaling_policy
    }
  }
}
