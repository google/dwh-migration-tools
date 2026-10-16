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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequestOnWaitTimeout;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.QueryTag;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a Databricks SQL Warehouse via the Statement Execution API using
 * {@link Disposition#EXTERNAL_LINKS} and {@link Format#JSON_ARRAY}, streaming chunks incrementally.
 * The calls throw the {@code DatabricksException} of the SDK when the API returns an error, and an
 * {@link IOException} when a statement fails or a chunk can't be downloaded.
 */
public final class DatabricksSqlHelper {

  /**
   * Key and value of the query tag attached to dumper SQL statements, so query history dumps skip
   * them.
   */
  public static final String QUERY_TAG_KEY = "source";

  public static final String QUERY_TAG_VALUE = "dwh-migration-dumper";

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlHelper.class);
  // 50s is the maximum synchronous wait of the API before polling.
  private static final String DEFAULT_WAIT_TIMEOUT = "50s";
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofSeconds(1);
  // The poll interval doubles after each poll of a running statement, up to this maximum.
  private static final long MAX_POLL_INTERVAL_MILLIS = 10_000L;
  private static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ofMinutes(30);
  private static final int MAX_DOWNLOAD_ATTEMPTS = 3;
  private static final long DOWNLOAD_RETRY_DELAY_MILLIS = 1_000L;
  private static final int TOO_MANY_REQUESTS_STATUS = 429;
  private static final int FIRST_SERVER_ERROR_STATUS = 500;
  private static final ObjectReader CHUNK_ROWS_READER =
      new ObjectMapper().readerFor(new TypeReference<List<List<String>>>() {});
  private static final ImmutableList<QueryTag> QUERY_TAGS =
      ImmutableList.of(new QueryTag().setKey(QUERY_TAG_KEY).setValue(QUERY_TAG_VALUE));

  /** Quotes SQL identifiers and string literals as Databricks prints them. */
  public static final class SqlQuoting {

    private static final Pattern PLAIN_IDENTIFIER_PATTERN =
        Pattern.compile("[A-Za-z0-9_]*[A-Za-z_][A-Za-z0-9_]*");

    private SqlQuoting() {}

    public static String quoteIdentifier(String identifier) {
      return "`" + identifier.replace("`", "``") + "`";
    }

    public static String quoteQualifiedName(String... parts) {
      return String.join(".", Lists.transform(Arrays.asList(parts), SqlQuoting::quoteIdentifier));
    }

    public static String quoteIdentifierIfNeeded(String identifier) {
      return PLAIN_IDENTIFIER_PATTERN.matcher(identifier).matches()
          ? identifier
          : quoteIdentifier(identifier);
    }

    public static String quoteStringLiteral(String value) {
      return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
  }

  /**
   * Streams the result rows of a SQL statement executed by {@link DatabricksSqlHelper}: the rows of
   * the first chunk of the response, then the rows of chunks 1 to {@code total_chunk_count - 1}.
   */
  public static final class StatementResult {

    private final DatabricksSqlHelper helper;
    private final String statementId;
    private final long totalChunkCount;
    private final ImmutableList<String> columnNames;
    private long nextChunkIndex = 1;
    private Iterator<List<String>> currentRows;

    private StatementResult(DatabricksSqlHelper helper, StatementResponse response)
        throws IOException {
      ResultManifest manifest = response.getManifest();
      this.helper = helper;
      this.statementId = response.getStatementId();
      this.totalChunkCount =
          manifest != null && manifest.getTotalChunkCount() != null
              ? manifest.getTotalChunkCount()
              : 0L;
      this.columnNames = findColumnNames(manifest);
      this.currentRows = helper.downloadRows(response.getResult()).iterator();
    }

    /**
     * Returns the cell at {@code index}, or null if {@code row} is null or {@code index} is out of
     * bounds.
     */
    public static String cell(List<String> row, int index) {
      return row == null || index < 0 || index >= row.size() ? null : row.get(index);
    }

    /** Returns the names of the result columns, in order. */
    public ImmutableList<String> columnNames() {
      return this.columnNames;
    }

    /** Returns the next result row, downloading the next chunk when needed, or null at the end. */
    public List<String> nextRow() throws IOException {
      while (!currentRows.hasNext()) {
        if (nextChunkIndex >= totalChunkCount) {
          return null;
        }
        currentRows = helper.downloadChunkRows(statementId, nextChunkIndex++).iterator();
      }
      return currentRows.next();
    }
  }

  private final WorkspaceClient workspaceClient;
  private final String warehouseId;
  private final HttpClient httpClient;
  private final long pollIntervalMillis;
  private final Duration statementTimeout;

  public DatabricksSqlHelper(WorkspaceClient workspaceClient, String warehouseId) {
    this(workspaceClient, warehouseId, DEFAULT_POLL_INTERVAL, DEFAULT_STATEMENT_TIMEOUT);
  }

  DatabricksSqlHelper(
      WorkspaceClient workspaceClient,
      String warehouseId,
      Duration pollInterval,
      Duration statementTimeout) {
    this.workspaceClient = Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    this.warehouseId = Preconditions.checkNotNull(warehouseId, "Warehouse ID was null.");
    this.httpClient = workspaceClient.apiClient().getHttpClient();
    this.pollIntervalMillis = pollInterval.toMillis();
    this.statementTimeout = statementTimeout;
  }

  /**
   * Executes {@code sql} in {@code catalog} and returns a stream over its result rows. Cancels the
   * statement and throws an {@link InterruptedIOException} if the thread is interrupted while the
   * statement runs.
   */
  public StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException {
    return new StatementResult(this, executeToCompletion(catalog, sql));
  }

  private List<List<String>> downloadChunkRows(String statementId, long chunkIndex)
      throws IOException {
    return downloadRows(
        workspaceClient.statementExecution().getStatementResultChunkN(statementId, chunkIndex));
  }

  private StatementResponse executeToCompletion(String catalog, String sql) throws IOException {
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(warehouseId)
            .setCatalog(catalog)
            .setStatement(sql)
            .setDisposition(Disposition.EXTERNAL_LINKS)
            .setFormat(Format.JSON_ARRAY)
            .setWaitTimeout(DEFAULT_WAIT_TIMEOUT)
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE)
            .setQueryTags(QUERY_TAGS);

    StatementResponse response = workspaceClient.statementExecution().executeStatement(request);
    String statementId = response.getStatementId();
    long deadlineNanos = System.nanoTime() + statementTimeout.toNanos();
    long sleepMillis = pollIntervalMillis;
    while (isRunningState(response.getStatus().getState())) {
      // sleepUninterruptibly() keeps the interrupt flag, so an interrupt is seen here after at most
      // one poll interval.
      if (Thread.currentThread().isInterrupted()) {
        cancelQuietly(statementId);
        throw new InterruptedIOException(
            String.format("Interrupted while waiting for SQL statement %s.", statementId));
      }

      if (System.nanoTime() - deadlineNanos >= 0L) {
        cancelQuietly(statementId);
        throw new IOException(
            String.format(
                "Timed out waiting for SQL statement %s after %s.", statementId, statementTimeout));
      }

      Uninterruptibles.sleepUninterruptibly(sleepMillis, MILLISECONDS);
      sleepMillis = min(sleepMillis * 2, MAX_POLL_INTERVAL_MILLIS);
      response = workspaceClient.statementExecution().getStatement(statementId);
    }

    StatementStatus status = response.getStatus();
    if (status.getState() != StatementState.SUCCEEDED) {
      // The error is set for the FAILED state only.
      ServiceError error = status.getError();
      throw new IOException(
          String.format(
              "SQL statement %s failed in state %s: %s",
              statementId, status.getState(), error == null ? null : error.getMessage()));
    }
    return response;
  }

  private List<List<String>> downloadRows(ResultData chunk) throws IOException {
    List<List<String>> rows = new ArrayList<>();
    if (chunk != null && chunk.getExternalLinks() != null) {
      for (ExternalLink link : chunk.getExternalLinks()) {
        rows.addAll(downloadLink(link));
      }
    }
    return rows;
  }

  // The SDK doesn't retry these downloads, and the pre-signed URLs must get only the headers of the
  // link, not the workspace token. The URL isn't written to the messages, because it's a secret.
  private List<List<String>> downloadLink(ExternalLink link) throws IOException {
    Request request = new Request(Request.GET, link.getExternalLink());
    if (link.getHttpHeaders() != null) {
      request.withHeaders(link.getHttpHeaders());
    }
    IOException failure = null;
    for (int attempt = 1; attempt <= MAX_DOWNLOAD_ATTEMPTS; attempt++) {
      if (failure != null) {
        if (Thread.currentThread().isInterrupted()) {
          // Throws the last failure without retrying; the interrupt flag stays set for the caller.
          break;
        }

        logger.warn("Retrying the download of a result chunk: {}", failure.getMessage());
        Uninterruptibles.sleepUninterruptibly(DOWNLOAD_RETRY_DELAY_MILLIS, MILLISECONDS);
      }
      Response response;
      try {
        response = httpClient.execute(request);
      } catch (IOException e) {
        failure = e;
        continue;
      }
      int status = response.getStatusCode();
      try (InputStream body = response.getBody()) {
        if (status >= 200 && status < 300) {
          return CHUNK_ROWS_READER.readValue(body);
        }
      }
      failure =
          new IOException(
              String.format("Failed to download external result chunk (HTTP %d).", status));
      if (status != TOO_MANY_REQUESTS_STATUS && status < FIRST_SERVER_ERROR_STATUS) {
        break;
      }
    }
    throw failure;
  }

  private static ImmutableList<String> findColumnNames(ResultManifest manifest) {
    ResultSchema schema = manifest != null ? manifest.getSchema() : null;
    if (schema == null || schema.getColumns() == null) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (ColumnInfo column : schema.getColumns()) {
      names.add(Strings.nullToEmpty(column.getName()));
    }
    return names.build();
  }

  private static boolean isRunningState(StatementState state) {
    return state == StatementState.PENDING || state == StatementState.RUNNING;
  }

  private void cancelQuietly(String statementId) {
    try {
      workspaceClient
          .statementExecution()
          .cancelExecution(new CancelExecutionRequest().setStatementId(statementId));
    } catch (RuntimeException e) {
      logger.debug("Ignored error cancelling statement {}: {}", statementId, e.getMessage());
    }
  }
}
