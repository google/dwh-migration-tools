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
package com.google.edwmigration.validation.core;

import autovalue.shaded.com.google.common.base.Preconditions;
import com.google.cloud.storage.*;
import com.google.common.io.Closer;
import com.google.edwmigration.validation.config.ValidationType;
import com.google.edwmigration.validation.connector.api.Connector;
import com.google.edwmigration.validation.connector.api.Handle;
import com.google.edwmigration.validation.connector.bigquery.BigQueryHandle;
import com.google.edwmigration.validation.connector.bigquery.BigQuerySetup;
import com.google.edwmigration.validation.connector.common.AbstractSourceTask;
import com.google.edwmigration.validation.connector.jdbc.JdbcHandle;
import com.google.edwmigration.validation.deformed.*;
import com.google.edwmigration.validation.io.GcsUpload;
import com.google.edwmigration.validation.io.writer.CsvWriterFactory;
import com.google.edwmigration.validation.io.writer.ResultSetWriterFactory;
import com.google.edwmigration.validation.model.Either;
import com.google.edwmigration.validation.model.ExecutionState;
import com.google.edwmigration.validation.model.Failure;
import com.google.edwmigration.validation.model.NamedTask;
import com.google.edwmigration.validation.model.UserInputContext;
import com.google.edwmigration.validation.util.BigQueryTableNamer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrator is responsible for composing and executing the full validation pipeline.
 *
 * <p>This class wires together tasks that perform configuration validation, source query execution,
 * GCS uploads, BigQuery setup, and result comparison. Each task is wrapped as a {@link NamedTask},
 * providing contextual logging and composability. The orchestrator does not execute the pipeline
 * itself—it simply defines task construction logic that can be consumed by an external controller
 * or runner.
 *
 * <p>Internally, tasks operate on either {@link UserInputContext} or {@link ExecutionState},
 * transitioning from static config inputs to dynamic runtime state. Error handling is built into
 * each task via the {@link Either} pattern: a {@code Left<Failure>} indicates failure with a
 * human-readable message, while {@code Right<T>} indicates success and progression to the next
 * stage.
 *
 * <p>The orchestrator assumes that connectors for both the source and target systems are provided
 * at construction time.
 */
public class Orchestrator {

  private static final Logger LOG = LoggerFactory.getLogger(Orchestrator.class);
  private final ResultSetWriterFactory writerFactory = new CsvWriterFactory();

  private final Connector sourceConnector;
  private final Connector targetConnector;

  public Orchestrator(Connector sourceConnector, Connector bqTargetConnector) {
    this.sourceConnector = sourceConnector;
    this.targetConnector = bqTargetConnector;
  }

  public NamedTask<UserInputContext, UserInputContext> validateUserInput() {
    return NamedTask.of(
        "Validate User Input", context -> validate(context).map(ignored -> context));
  }

  private Either<Failure, UserInputContext> validate(UserInputContext ctx) {
    Deformed<UserInputContext> validator = new Deformed<>(UserInputContext.buildSchema());
    validator.validate(ctx);
    if (!validator.isValid) {
      return Either.left(Failure.CONFIG.with(validator.validationState.toJson()));
    }
    return Either.right(ctx);
  }

  public NamedTask<UserInputContext, ExecutionState> initExecutionState() {
    return NamedTask.of(
        "Initialize Execution State", context -> Either.right(ExecutionState.of(context)));
  }

  public NamedTask<ExecutionState, ExecutionState> prepareOutputPath() {
    return NamedTask.of(
        "Prepare Output Path",
        state -> {
          try {
            Path path = Paths.get("validationOutputs");
            if (!Files.exists(path)) {
              Files.createDirectories(path);
            }
            return Either.right(state.map(s -> s.withOutputUri(path.toUri())));
          } catch (Exception e) {
            return Either.left(Failure.IO.with("Prepare Output Path failed: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> registerHandlers() {
    return NamedTask.of(
        "Register Handlers",
        state -> {
          try {
            Preconditions.checkState(state.closer == null, "Closer already registered");
            Closer closer = Closer.create();
            Handle sourceHandle = closer.register(sourceConnector.open(state));
            Handle targetHandle = closer.register(targetConnector.open(state));
            return Either.right(
                state.map(
                    s ->
                        s.withCloser(closer)
                            .withSourceHandle((JdbcHandle) sourceHandle)
                            .withTargetHandle((BigQueryHandle) targetHandle)));
          } catch (Exception e) {
            return Either.left(
                Failure.SQL.with("Unable to register handlers: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> sourceQuery() {
    return NamedTask.of(
        "Run Source Queries",
        state -> {
          try {
            AbstractSourceTask task = sourceConnector.getSourceQueryTask(state, writerFactory);
            task.run();

            ResultSetMetaData aggMeta = task.getAggregateQueryMetadata();
            ResultSetMetaData rowMeta = task.getRowQueryMetadata();

            return Either.right(
                state.map(s -> s.withAggMetadata(aggMeta).withRowMetadata(rowMeta)));
          } catch (Exception e) {
            return Either.left(Failure.SQL.with("Run Source Queries failed: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> uploadToGcs() {
    return NamedTask.of(
        "Upload to GCS",
        state -> {
          try {
            String gcsPath = state.context.googleCloudStorage.gcsPath;
            String projectId = state.context.googleCloudStorage.projectId;

            Storage storage =
                StorageOptions.newBuilder().setProjectId(projectId).build().getService();

            Either<Failure, HashMap<ValidationType, String>> uploadResult =
                GcsUpload.uploadToGcs(storage, state.outputUri, gcsPath, state.context);

            // The GCS upload returns an Either containing either a Failure or the uploaded URIs.
            // To propagate this into the ExecutionState, we first map over the Either to access
            // the success case, then map over the ExecutionState to update it. Since Java lacks
            // native support for applicative functors or lifting, this nested mapping is a bit
            // awkward but necessary here.
            return uploadResult.map(
                uploadedUris -> state.map(s -> s.withUploadedGcsUris(uploadedUris)));
          } catch (Exception e) {
            return Either.left(
                Failure.GCS_UPLOAD.with("Upload to GCS failed: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> setupBigQueryAggregateTable() {
    return NamedTask.of(
        "Setup BigQuery Aggregate Table",
        state -> {
          try {
            BigQuerySetup.createBqExternalTable(
                state.aggMetadata,
                state.context.bqTargetTable.dataset,
                state.uploadedGcsUris.get(ValidationType.AGGREGATE),
                BigQueryTableNamer.getSourceTableName(state.context, ValidationType.AGGREGATE),
                state.context.bqTargetTable.projectId);
            return Either.right(state);
          } catch (SQLException e) {
            return Either.left(
                Failure.SQL.with("Failed to create BigQuery external table: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> setupBigQueryRowTable() {
    return NamedTask.of(
        "Setup BigQuery Row Table",
        state -> {
          try {
            BigQuerySetup.createBqExternalTable(
                state.rowMetadata,
                state.context.bqTargetTable.dataset,
                state.uploadedGcsUris.get(ValidationType.ROW),
                BigQueryTableNamer.getSourceTableName(state.context, ValidationType.ROW),
                state.context.bqTargetTable.projectId);
            return Either.right(state);
          } catch (SQLException e) {
            return Either.left(
                Failure.SQL.with("Failed to create BigQuery external table: " + e.getMessage(), e));
          }
        });
  }

  public NamedTask<ExecutionState, ExecutionState> targetQuery() {
    return NamedTask.of(
        "Run Query against Target BQ",
        state -> {
          //   AbstractTargetTask targetQueryTask = targetConnector.getTargetQueryTask(state);
          //   LOG.debug("Created targetQueryTask -- ", targetQueryTask);
          //   targetQueryTask.run();

          // TODO what do we do with the targetQueryTask state now?
          LOG.warn("Run Query against Target BQ task not yet implemented");
          return Either.right(state);
        });
  }

  public NamedTask<ExecutionState, ExecutionState> createResultsTable() {
    return NamedTask.of(
        "Create Results Table",
        state -> {
          LOG.warn("Create results table task not yet implemented.");
          return Either.right(state);
        });
  }

  public NamedTask<ExecutionState, ExecutionState> aggregateComparisonQuery() {
    return NamedTask.of(
        "Run Aggregate Comparison Query",
        state -> {
          LOG.warn("Aggregate comparison query task not yet implemented.");
          return Either.right(state);
        });
  }

  public NamedTask<ExecutionState, ExecutionState> computeColumnMappings() {
    return NamedTask.of(
        "Compute Column Mappings",
        state -> {
          LOG.warn("Compute column mappings task not yet implemented.");
          return Either.right(state);
        });
  }

  public NamedTask<ExecutionState, ExecutionState> rowComparisonQuery() {
    return NamedTask.of(
        "Run Row Comparison Query",
        state -> {
          LOG.warn("Row comparison query task not yet implemented.");
          return Either.right(state);
        });
  }

  public NamedTask<ExecutionState, ExecutionState> cleanupResources() {
    return NamedTask.of(
        "Cleanup Resources",
        state -> {
          try {
            if (state.closer != null) {
              state.closer.close();
            }
            return Either.right(state);
          } catch (Exception e) {
            return Either.left(Failure.IO.with("Failed to close resources: " + e.getMessage(), e));
          }
        });
  }
}
