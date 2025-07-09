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
package com.google.edwmigration.dtsstatus.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.edwmigration.dtsstatus.exception.MalformedMessageException;
import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.protobuf.Timestamp;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageTranslator {

  private static final Logger logger = LoggerFactory.getLogger(MessageTranslator.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String MESSAGE_HEADER = "table status: ";

  public Optional<TransferStatus> translateLogMessage(String messageText, Timestamp timestamp) {
    Objects.requireNonNull(messageText);
    Objects.requireNonNull(timestamp);
    if (messageText.startsWith(MESSAGE_HEADER)) {
      String json = messageText.substring(MESSAGE_HEADER.length());
      try {
        JsonNode root = objectMapper.readTree(json);
        String database = root.path("source").path("database").asText();
        if (Strings.isNullOrEmpty(database)) {
          throw new MalformedMessageException("Missing source.database value");
        }
        String table = root.path("source").path("table").asText();
        if (Strings.isNullOrEmpty(table)) {
          throw new MalformedMessageException("Missing source.table value");
        }
        String status = root.path("status").asText();
        if (Strings.isNullOrEmpty(status)) {
          throw new MalformedMessageException("Missing status value");
        }
        return Optional.of(new TransferStatus(database, table, status, timestamp));
      } catch (JsonProcessingException | MalformedMessageException e) {
        logger.warn("Failed to parse log message: {}", messageText, e);
      }
    }
    logger.warn("Unknown log message: {}", messageText);
    return Optional.empty();
  }
}
