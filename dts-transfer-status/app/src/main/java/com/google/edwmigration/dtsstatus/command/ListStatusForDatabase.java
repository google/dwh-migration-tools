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

import com.google.cloud.logging.v2.LoggingClient;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.consumer.TransferStatusConsumer;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.edwmigration.dtsstatus.util.MessageTranslator;
import com.google.logging.v2.ListLogEntriesRequest;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListStatusForDatabase {

  private static final Logger logger = LoggerFactory.getLogger(ListStatusForDatabase.class);
  private final String projectId;
  private final String database;
  private final String logFilter;
  private final Supplier<LoggingClient> clientSupplier;
  private final Consumer<TransferStatus> statusConsumer;
  private final MessageTranslator messageTranslator;
  private static final String LOG_FILTER_TEMPLATE =
      "jsonPayload.message:\"table status\" AND jsonPayload.message:\"\\\"database\\\":\\\"{0}\\\"\"";

  ListStatusForDatabase(
      StatusOptions options,
      Supplier<LoggingClient> clientSupplier,
      Consumer<TransferStatus> statusConsumer,
      MessageTranslator messageTranslator) {
    validateOptions(options);
    this.projectId = options.getProjectId();
    this.database = options.getDatabase();
    this.clientSupplier = clientSupplier;
    this.statusConsumer = statusConsumer;
    this.messageTranslator = messageTranslator;
    this.logFilter = MessageFormat.format(LOG_FILTER_TEMPLATE, database);
  }

  public void run() {
    try (LoggingClient loggingClient = clientSupplier.get()) {
      logger.info("Listing statuses for database {}", database);
      ListLogEntriesRequest request =
          ListLogEntriesRequest.newBuilder()
              .addResourceNames("projects/" + projectId)
              .setFilter(logFilter)
              .build();
      Stream<Optional<TransferStatus>> statuses =
          StreamSupport.stream(
                  loggingClient.listLogEntries(request).iterateAll().spliterator(), false)
              .map(
                  entry ->
                      messageTranslator.translateLogMessage(
                          entry.getJsonPayload().getFieldsMap().get("message").getStringValue(),
                          entry.getTimestamp()));
      consumeLatestExistingStatuses(statuses, statusConsumer);
    }
  }

  public static ListStatusForDatabase instance(
      StatusOptions options, TransferStatusConsumer transferStatusConsumer) {
    return new ListStatusForDatabase(
        options,
        ListStatusForDatabase::createClient,
        transferStatusConsumer,
        new MessageTranslator());
  }

  private static LoggingClient createClient() {
    try {
      return LoggingClient.create();
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create LoggingClient", ex);
    }
  }

  private static void validateOptions(StatusOptions options) {
    if (!options.hasProjectId() || options.getProjectId().isEmpty()) {
      throw new InvalidArgumentException("Project ID is required");
    }
    if (!options.hasDatabase()) {
      throw new InvalidArgumentException("Database is required");
    }
  }
}
