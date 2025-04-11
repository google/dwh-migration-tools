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
package com.google.edwmigration.dtsstatus.command;

import static com.google.edwmigration.dtsstatus.util.MessageHandler.consumeLatestExistingStatuses;

import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfigName;
import com.google.common.base.Strings;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.consumer.TransferStatusConsumer;
import com.google.edwmigration.dtsstatus.exception.GcpClientException;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.edwmigration.dtsstatus.util.MessageTranslator;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListStatusForConfig {

  private static final Logger logger = LoggerFactory.getLogger(ListStatusForConfig.class);
  private final String projectId;
  private final String location;
  private final String configId;
  private final Supplier<DataTransferServiceClient> clientSupplier;
  private final Consumer<TransferStatus> statusConsumer;
  private final MessageTranslator messageTranslator;

  ListStatusForConfig(
      StatusOptions options,
      Supplier<DataTransferServiceClient> clientSupplier,
      Consumer<TransferStatus> statusConsumer,
      MessageTranslator messageTranslator) {
    validateOptions(options);
    this.projectId = options.getProjectId();
    this.location = options.getLocation();
    this.configId = options.getConfigId();
    this.clientSupplier = clientSupplier;
    this.statusConsumer = statusConsumer;
    this.messageTranslator = messageTranslator;
  }

  public void run() {
    try (DataTransferServiceClient client = clientSupplier.get()) {
      logger.info("Listing statuses for config {}", configId);
      TransferConfigName transferConfigName =
          TransferConfigName.ofProjectLocationTransferConfigName(projectId, location, configId);
      Stream<Optional<TransferStatus>> statuses =
          StreamSupport.stream(
                  client.listTransferRuns(transferConfigName).iterateAll().spliterator(), false)
              .flatMap(
                  run ->
                      StreamSupport.stream(
                          client.listTransferLogs(run.getName()).iterateAll().spliterator(), false))
              .filter(message -> message.getMessageText().startsWith("table status: "))
              .map(
                  message ->
                      messageTranslator.translateLogMessage(
                          message.getMessageText(), message.getMessageTime()));
      consumeLatestExistingStatuses(statuses, statusConsumer);
    } catch (Exception ex) {
      logger.error("Failed to list statuses for config", ex);
    }
  }

  public static ListStatusForConfig instance(
      StatusOptions options, TransferStatusConsumer transferStatusConsumer) {
    return new ListStatusForConfig(
        options,
        ListStatusForConfig::createClient,
        transferStatusConsumer,
        new MessageTranslator());
  }

  private static DataTransferServiceClient createClient() {
    try {
      return DataTransferServiceClient.create();
    } catch (IOException e) {
      throw new GcpClientException("Failed to create DataTransferServiceClient", e);
    }
  }

  private static void validateOptions(StatusOptions options) {
    if (!options.hasProjectId() || Strings.isNullOrEmpty(options.getProjectId())) {
      throw new InvalidArgumentException("Project ID is required");
    }
    if (!options.hasLocation() || Strings.isNullOrEmpty(options.getLocation())) {
      throw new InvalidArgumentException("Location is required");
    }
    if (!options.hasConfigId() || Strings.isNullOrEmpty(options.getConfigId())) {
      throw new InvalidArgumentException("Config ID is required");
    }
  }
}
