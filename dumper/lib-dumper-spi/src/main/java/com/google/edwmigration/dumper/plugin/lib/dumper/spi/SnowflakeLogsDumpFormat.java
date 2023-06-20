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

/** @author matt */
public interface SnowflakeLogsDumpFormat {

  String FORMAT_NAME = "snowflake.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

  enum Header {
    DatabaseName,
    SchemaName,
    UserName,
    WarehouseName,
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

  interface WarehouseEventsHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_events_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "automatic_clustering_history.csv";

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
      DatabaseName
    }
  }

  interface CopyHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "copy_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "database_replication_usage_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "login_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "metering_daily_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "pipe_usage_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "query_acceleration_history.csv";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      WarehouseId,
      WarehouseName
    }
  }

  interface ReplicationGroupUsageHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "replication_group_usage_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "serverless_task_history.csv";

    enum Header {
      StartTime,
      EndTime,
      CreditsUsed,
      TaskId,
      TaskName,
      SchemaId,
      SchemaName,
      DatabaseId,
      DatabaseName
    }
  }

  interface TaskHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "task_history.csv";

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
      ScheduledFrom
    }
  }

  interface WarehouseLoadHistoryFormat {

    String AU_ZIP_ENTRY_NAME = "warehouse_load_history.csv";

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

    String AU_ZIP_ENTRY_NAME = "warehouse_metering_history.csv";

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
