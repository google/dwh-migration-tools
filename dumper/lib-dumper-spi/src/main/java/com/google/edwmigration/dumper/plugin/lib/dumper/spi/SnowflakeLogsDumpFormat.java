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

/** @author matt */
public interface SnowflakeLogsDumpFormat {

  String FORMAT_NAME = "snowflake.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

  enum Header {
    DatabaseName,
    SchemaName,
    UserName,
    WarehouseName,
    QueryId,
    SessionId,
    QueryType,
    ExecutionStatus,
    ErrorCode,
    StartTime,
    EndTime,
    TotalElapsedTime,
    BytesScanned,
    RowsProduced,
    CreditsUsedCloudServices,
    QueryText
  }

  interface QueryHistoryExtendedFormat {
    String ZIP_ENTRY_PREFIX = "query_history-au_";

    enum Header {
      QueryId,
      QueryText,
      DatabaseName,
      SchemaName,
      QueryType,
      SessionId,
      UserName,
      WarehouseName,
      ClusterNumber,
      QueryTag,
      ExecutionStatus,
      ErrorCode,
      ErrorMessage,
      StartTime,
      EndTime,
      BytesScanned,
      PercentageScannedFromCache,
      BytesWritten,
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
      TotalElapsedTime,
      CompilationTime,
      ExecutionTime,
      QueuedProvisioningTime,
      QueuedRepairTime,
      QueuedOverloadTime,
      TransactionBlockedTime,
      ListExternalFilesTime,
      CreditsUsedCloudServices,
      QueryLoadPercent,
      QueryAccelerationBytesScanned,
      QueryAccelerationPartitionsScanned,
      ChildQueriesWaitTime,
      TransactionId,
    }
  }

  interface WarehouseEventsHistoryFormat {

    String ZIP_ENTRY_PREFIX = "warehouse_events_history_";

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

    String ZIP_ENTRY_PREFIX = "automatic_clustering_history-au_";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      NumBytesReclustered,
      NumRowsReclustered,
      TableId,
      TableName,
      SchemaId,
      SchemaName,
      DatabaseId,
      DatabaseName,
      InstanceId
    }
  }

  interface CopyHistoryFormat {

    String ZIP_ENTRY_PREFIX = "copy_history-au_";

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
      TableId,
      TableName,
      TableSchemaId,
      TableSchemaName,
      TableCatalogId,
      TableCatalogName,
      PipeCatalogName,
      PipeSchemaName,
      PipeName,
      PipeReceivedTime,
      FirstCommitTime
    }
  }

  interface DatabaseReplicationUsageHistoryFormat {

    String ZIP_ENTRY_PREFIX = "database_replication_usage_history-au_";

    enum Header {
      StartTime,
      EndTime,
      DatabaseName,
      DatabaseId,
      CreditsUsed,
      BytesTransferred
    }
  }

  interface LoginHistoryFormat {

    String ZIP_ENTRY_PREFIX = "login_history-au_";

    enum Header {
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

    String ZIP_ENTRY_PREFIX = "metering_daily_history_";

    enum Header {
      ServiceType,
      UsageDate,
      CreditsUsedCompute,
      CreditsUsedCloudServices,
      CreditsUsed,
      CreditsAdjustmentCloudServices,
      CreditsBilled
    }
  }

  interface PipeUsageHistoryFormat {

    String ZIP_ENTRY_PREFIX = "pipe_usage_history-au_";

    enum Header {
      PipeId,
      PipeName,
      StartTime,
      EndTime,
      CreditsUsed,
      BytesInserted,
      FilesInserted
    }
  }

  interface QueryAccelerationHistoryFormat {

    String ZIP_ENTRY_PREFIX = "query_acceleration_history-au_";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      WarehouseId,
      WarehouseName
    }
  }

  interface ReplicationGroupUsageHistoryFormat {

    String ZIP_ENTRY_PREFIX = "replication_group_usage_history-au_";

    enum Header {
      StartTime,
      EndTime,
      ReplicationGroupName,
      ReplicationGroupId,
      CreditsUsed,
      BytesTransferred
    }
  }

  interface ServerlessTaskHistoryFormat {

    String ZIP_ENTRY_PREFIX = "serverless_task_history-au_";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      TaskId,
      TaskName,
      SchemaId,
      SchemaName,
      DatabaseId,
      DatabaseName,
      InstanceId
    }
  }

  interface TaskHistoryFormat {

    String ZIP_ENTRY_PREFIX = "task_history-au_";

    enum Header {
      Name,
      QueryText,
      ConditionText,
      SchemaName,
      TaskSchemaId,
      DatabaseName,
      TaskDatabaseId,
      ScheduledTime,
      CompletedTime,
      State,
      ReturnValue,
      QueryId,
      QueryStartTime,
      ErrorCode,
      ErrorMessage,
      GraphVersion,
      RunId,
      RootTaskId,
      ScheduledFrom,
      InstanceId,
      AttemptNumber,
      Config
    }
  }

  interface WarehouseLoadHistoryFormat {

    String ZIP_ENTRY_PREFIX = "warehouse_load_history-au_";

    enum Header {
      StartTime,
      EndTime,
      WarehouseId,
      WarehouseName,
      AvgRunning,
      AvgQueuedLoad,
      AvgQueuedProvisioning,
      AvgBlocked
    }
  }

  interface WarehouseMeteringHistoryFormat {

    String ZIP_ENTRY_PREFIX = "warehouse_metering_history-au_";

    enum Header {
      StartTime,
      EndTime,
      WarehouseId,
      WarehouseName,
      CreditsUsed,
      CreditsUsedCompute,
      CreditsUsedCloudServices
    }
  }
}
