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

import com.google.edwmigration.validation.connector.api.Connector;
import com.google.edwmigration.validation.connector.registry.ConnectorRegistry;
import com.google.edwmigration.validation.model.Either;
import com.google.edwmigration.validation.model.ExecutionState;
import com.google.edwmigration.validation.model.Failure;
import com.google.edwmigration.validation.model.TaskPipeline;
import com.google.edwmigration.validation.model.UserInputContext;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task orchestration for running validations */
public class Validator {
  private final UserInputContext context;
  private static final Logger LOG = LoggerFactory.getLogger(Validator.class);

  public Validator(UserInputContext context) {
    this.context = context;
  }

  public boolean run() {
    Connector sourceConnector =
        ConnectorRegistry.getInstance().getByName(context.sourceConnection.connectionType);
    Connector targetConnector = ConnectorRegistry.getInstance().getByName("bigquery");
    Orchestrator orchestrator = new Orchestrator(sourceConnector, targetConnector);
    Optional<ExecutionState> executionState = Optional.empty();

    TaskPipeline<UserInputContext, ExecutionState> pipeline =
        TaskPipeline.<UserInputContext>start()
            .then(orchestrator.validateUserInput())
            .then(orchestrator.initExecutionState())
            .then(orchestrator.prepareOutputPath())
            .then(orchestrator.registerHandlers())
            .then(orchestrator.sourceQuery())
            .then(orchestrator.uploadToGcs())
            .then(orchestrator.setupBigQueryAggregateTable())
            .then(orchestrator.setupBigQueryRowTable())
            .then(orchestrator.targetQuery())
            .then(orchestrator.createResultsTable())
            .then(orchestrator.aggregateComparisonQuery())
            .then(orchestrator.computeColumnMappings())
            .then(orchestrator.rowComparisonQuery());

    try {
      Either<Failure, ExecutionState> result = pipeline.run(context);

      if (result.isRight()) {
        ExecutionState state = result.getRight();
        executionState = Optional.of(state);
        LOG.info("Validation succeeded.");
        return true;
      } else {
        LOG.error("Validation failed: " + result.getLeft());
        return false;
      }
    } finally {
      executionState.ifPresent(
          state -> {
            if (state.closer != null) {
              try {
                state.closer.close();
                LOG.debug("Resources cleaned up successfully.");
              } catch (Exception e) {
                LOG.error("Failed to close resources", e);
              }
            }
          });
    }
  }
}
