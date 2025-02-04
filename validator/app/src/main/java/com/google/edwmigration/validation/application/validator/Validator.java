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
package com.google.edwmigration.validation.application.validator;

import com.google.edwmigration.validation.application.validator.connector.Connector;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class Validator {
  private static final Logger LOG = LoggerFactory.getLogger(Validator.class);

  public boolean run(String... args) throws Exception {
    ValidationArguments validationArguments = new ValidationArguments(args);
    return run(validationArguments);
  }

  public boolean run(@Nonnull ValidationArguments arguments) throws Exception {
    String sourceConnectionName = arguments.getSourceConnection().getConnectionType();
    String targetConnectionName = arguments.getTargetConnection().getConnectionType();
    if (sourceConnectionName == null) {
      LOG.error("Source connection type is required.");
      return false;
    } else if (targetConnectionName == null) {
      LOG.error("Target connection type is required.");
      return false;
    }

    Connector sourceConnector = ConnectorRepository.getInstance().getByName(sourceConnectionName);
    if (sourceConnector == null) {
      LOG.error(
          "Source DB '{}' not supported; available are {}.",
          sourceConnectionName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }

    Connector targetConnector = ConnectorRepository.getInstance().getByName(targetConnectionName);
    if (targetConnector == null) {
      LOG.error(
          "Target DB '{}' not supported; available are {}.",
          targetConnectionName,
          ConnectorRepository.getInstance().getAllNames());
      return false;
    }

    return true;
  }

  protected boolean run(
      @Nonnull ValidationArguments arguments, Connector sourceConnector, Connector targetConnector)
      throws Exception {
    LOG.info(
        "Using source connector" + sourceConnector + " and target connector " + targetConnector);
    Handle sourceHandle = sourceConnector.open(arguments.getSourceConnection());
    Handle targetHandle = targetConnector.open(arguments.getTargetConnection());

    return true;
  }
}
