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

/** Output archive contract of the Databricks logs connector. */
public interface DatabricksLogsDumpFormat {

  String FORMAT_NAME = "databricks.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

  /**
   * Columns of the query history files, named after {@code system.query.history}. New columns are
   * added at the end, so that the columns of earlier dumps keep their positions.
   *
   * <p>The files have only the queries that ended: {@link #ExecutionStatus} is {@code FINISHED},
   * {@code FAILED} or {@code CANCELED}. These are the only termination states that {@code
   * system.query.history} documents, and the queries of the query history REST API that are still
   * queued or running are skipped.
   *
   * <p>The query history REST API doesn't have every value. The dumps that are read from it leave
   * {@link #ComputeType}, {@link #WaitingAtCapacityDurationMs} and {@link
   * #WaitingForComputeDurationMs} empty. They also leave {@link #AccountId} empty when the
   * Databricks SDK can't find the account, and {@link #WorkspaceId} when the API sends no workspace
   * ID and the SDK can't find the workspace of the URL.
   *
   * <p>Databricks truncates long SQL statements in both {@code system.query.history} and the query
   * history REST API: {@link #StatementText} is capped at 153,600 characters (150 KiB), or around
   * 64–84 KiB when the compressed statement reaches the storage limit, and ends with {@code "..."}.
   */
  enum Header {
    AccountId,
    WorkspaceId,
    StatementId,
    SessionId,
    ExecutedBy,
    StatementText,
    StatementType,
    ExecutionStatus,
    ErrorMessage,
    WarehouseId,
    /**
     * Empty when reading the query history REST API, which has no compute type and uses placeholder
     * warehouse IDs (such as {@code 0000000000000000} and {@code 0000000000000001}) for serverless
     * compute; compute type for API dumps is left to downstream logic.
     */
    ComputeType,
    StartTime,
    EndTime,
    TotalDurationMs,
    CompilationDurationMs,
    ExecutionDurationMs,
    /** Empty when reading the query history REST API, which has no duration of this wait. */
    WaitingAtCapacityDurationMs,
    ReadBytes,
    ReadRows,
    ProducedRows,
    WrittenBytes,
    WrittenRows,
    SpilledLocalBytes,
    ClientApplication,
    /**
     * The time that the query waited for compute to be provisioned. Empty when reading the query
     * history REST API, which has no duration of this wait.
     */
    WaitingForComputeDurationMs,
    /**
     * {@code true} if the result was fetched from the result cache, and {@code false} if it wasn't.
     * Empty when the query history REST API sends no metrics for the query.
     */
    FromResultCache
  }
}
