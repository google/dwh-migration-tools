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
      StartTime,
      EndTime,
      TableName,
      CreditsUsed,
      NumBytesReclustered,
      NumRowsReclustered
    }
  }

  interface CopyHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "copy_history-au.csv";

    enum Header {
      FileName,
      StageLocation,
      LastLoadTime,
      RowCount,
      RowParsed,
      FileSize,
      FirstErrorMessage,
      FirstErrorLineNumber,
      FirstErrorCharacterPos,
      FirstErrorColumnName,
      ErrorCount,
      ErrorLimit,
      Status,
      TableCatalogName,
      TableSchemaName,
      TableName,
      PipeCatalogName,
      PipeSchemaName,
      PipeName,
      PipeReceivedTime
    }
  }

  interface DatabaseReplicationUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "database_replication_usage_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      DatabaseName,
      CreditsUsed,
      BytesTransferred
    }
  }

  interface LoginHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "login_history-au.csv";

    enum Header {
      ReaderAccountName,
      EventId,
      EventTimestamp,
      EventType,
      UserName,
      ClientIp,
      ReportedClientType,
      ReportedClientVersion,
      FirstAuthenticationFactor,
      SecondAuthenticationFactor,
      IsSuccess,
      ErrorCode,
      ErrorMessage,
      RelatedEventId,
      Connection
    }
  }

  interface MeteringDailyHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "metering_daily_history-au.csv";

    enum Header {
      ServiceType,
      OrganizationName,
      AccountName,
      AccountLocator,
      UsageDate,
      CreditsUsedCompute,
      CreditsUsedCloudServices,
      CreditsUsed,
      CreditsAdjustmentCloudServices,
      CreditsBilled,
      Region
    }
  }

  interface PipeUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "pipe_usage_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      PipeName,
      CreditsUsed,
      BytesInserted,
      FilesInserted
    }
  }

  interface QueryAccelerationHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "query_acceleration_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      WarehouseName,
      NumFilesScanned,
      NumBytesScanned
    }
  }

  interface QueryHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "query_history-au.csv";

    enum Header {
      ReaderAccountName,
      QueryId,
      QueryText,
      DatabaseId,
      DatabaseName,
      SchemaId,
      SchemaName,
      QueryType,
      SessionId,
      UserName,
      RoleName,
      WarehouseId,
      WarehouseName,
      WarehouseSize,
      WarehouseType,
      ClusterNumber,
      QueryTag,
      ExecutionStatus,
      ErrorCode,
      ErrorMessage,
      StartTime,
      EndTime,
      TotalElapsedTime,
      BytesScanned,
      PercentageScannedFromCache,
      BytesWritten,
      BytesWrittenToResult,
      BytesReadFromResult,
      RowsProduced,
      RowsInserted,
      RowsUpdated,
      RowsDeleted,
      RowsUnloaded,
      BytesDeleted,
      PartitionsScanned,
      PartitionsTotal,
      BytesSpilledToLocalStorage,
      BytesSpilledToRemoteStorage,
      BytesSentOverTheNetwork,
      CompilationTime,
      ExecutionTime,
      QueuedProvisioningTime,
      QueuedRepairTime,
      QueuedOverloadTime,
      TransactionBlockedTime,
      OutboundDataTransferCloud,
      OutboundDataTransferRegion,
      OutboundDataTransferBytes,
      InboundDataTransferCloud,
      InboundDataTransferRegion,
      InboundDataTransferBytes,
      ListExternalFilesTime,
      CreditsUsedCloudServices,
      ReleaseVersion,
      ExternalFunctionTotalInvocations,
      ExternalFunctionTotalSentRows,
      ExternalFunctionTotalReceivedRows,
      ExternalFunctionTotalSentBytes,
      ExternalFunctionTotalReceivedBytes,
      QueryLoadPercent,
      IsClientGeneratedStatement,
      QueryAccelerationBytesScanned,
      QueryAccelerationPartitionsScanned,
      QueryAccelerationUpperLimitScaleFactor,
      ChildQueriesWaitTime,
      Owner,
      OwnerRoleType,
      TransactionId,
      RoleType
    }
  }

  interface ReplicationGroupUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "replication_group_usage_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      ReplicationGroupName,
      CreditsUsed,
      BytesTransferred
    }
  }

  interface ServerlessTaskHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "serverless_task_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      TaskName,
      CreditsUsed
    }
  }

  interface TableStorageMetricsFormat {

    String AU_ZIP_ENTRY_NAME = "table_storage_metrics-au.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      Id,
      CloneGroupId,
      IsTransient,
      ActiveBytes,
      TimeTravelBytes,
      FailsafeBytes,
      RetainedForCloneBytes,
      TableCreated,
      TableDropped,
      TableEnteredFailsafe,
      CatalogCreated,
      CatalogDropped,
      SchemaCreated,
      SchemaDropped,
      Comment
    }
  }

  interface TaskHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "task_history-au.csv";

    enum Header {
      QueryId,
      Name,
      DatabaseName,
      SchemaName,
      QueryText,
      ConditionText,
      State,
      ErrorCode,
      ErrorMessage,
      ScheduledTime,
      QueryStartTime,
      NextScheduledTime,
      CompletedTime,
      RootTaskId,
      GraphVersion,
      RunId,
      ReturnValue,
      ScheduledFrom
    }
  }

  interface WarehouseLoadHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_load_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      WarehouseName,
      AvgRunning,
      AvgQueuedLoad,
      AvgQueuedProvisioning,
      AvgBlocked
    }
  }

  interface WarehouseMeteringHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_metering_history-au.csv";

    enum Header {
      StartTime,
      EndTime,
      WarehouseName,
      CreditsUsed,
      CreditsUsedCompute,
      CreditsUsedCloudServices
    }
  }

  interface WarehousesFormat {

    String AU_ZIP_ENTRY_NAME = "warehouses-au.csv";

    enum Header {
      Name,
      State,
      Type,
      Size,
      MinClusterCount,
      MaxClusterCount,
      StartedClusters,
      Running,
      Queued,
      IsDefault,
      IsCurrent,
      AutoSuspend,
      AutoResume,
      Available,
      Provisioning,
      Quiescing,
      Other,
      CreatedOn,
      ResumedOn,
      UpdatedOn,
      Owner,
      Comment,
      EnableQueryAcceleration,
      QueryAccelerationMaxScaleFactor,
      ResourceMonitor,
      ScalingPolicy
    }
  }
}
