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

import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.LocationName;
import com.google.common.base.Strings;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.consumer.TransferConfigConsumer;
import com.google.edwmigration.dtsstatus.exception.GcpClientException;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferConfig;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTransferConfigs {

  private static final Logger logger = LoggerFactory.getLogger(ListTransferConfigs.class);
  private final Supplier<DataTransferServiceClient> clientSupplier;
  private final String projectId;
  private final String location;
  private final Consumer<TransferConfig> configConsumer;

  ListTransferConfigs(
      StatusOptions options,
      Supplier<DataTransferServiceClient> clientSupplier,
      Consumer<TransferConfig> configConsumer) {
    validateOptions(options);
    this.projectId = options.getProjectId();
    this.location = options.getLocation();
    this.clientSupplier = clientSupplier;
    this.configConsumer = configConsumer;
  }

  public void run() {
    try (DataTransferServiceClient client = clientSupplier.get()) {
      LocationName locationName = LocationName.of(projectId, location);
      logger.info("Listing transfer configs for project: {}", projectId);
      Iterable<com.google.cloud.bigquery.datatransfer.v1.TransferConfig> configs =
          client.listTransferConfigs(locationName).iterateAll();

      StreamSupport.stream(configs.spliterator(), false)
          .map(ListTransferConfigs::mapConfig)
          .forEach(configConsumer);
    } catch (Exception ex) {
      logger.error("Failed to list transfer configs", ex);
    }
  }

  public static ListTransferConfigs instance(
      StatusOptions options, TransferConfigConsumer transferConfigConsumer) {
    return new ListTransferConfigs(
        options, ListTransferConfigs::createClient, transferConfigConsumer);
  }

  private static TransferConfig mapConfig(
      com.google.cloud.bigquery.datatransfer.v1.TransferConfig dtsConfig) {
    return new TransferConfig(dtsConfig.getName().split("/")[5], dtsConfig.getDisplayName());
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
  }
}
