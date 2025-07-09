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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.logging.v2.LoggingClient;
import com.google.cloud.logging.v2.LoggingClient.ListLogEntriesPagedResponse;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.edwmigration.dtsstatus.util.MessageTranslator;
import com.google.logging.v2.LogEntry;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListStatusForDatabaseTest {

  @Test
  void run_listsStatusesCorrectly() {
    StatusOptions options =
        new StatusOptions(new String[] {"--project-id", "project1", "--database", "db1"});
    LoggingClient mockClient = mock(LoggingClient.class);
    ListLogEntriesPagedResponse mockResponse = mock(ListLogEntriesPagedResponse.class);
    when(mockResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                logMessage("tab1", "s2", 2),
                logMessage("tab1", "s1", 1),
                logMessage("tab2", "s1", 3),
                logMessage("tab2", "s3", 4)));
    when(mockClient.listLogEntries(any())).thenReturn(mockResponse);

    List<TransferStatus> result = new ArrayList<>();

    ListStatusForDatabase command =
        new ListStatusForDatabase(options, () -> mockClient, result::add, new MessageTranslator());

    command.run();

    assertThat(result.size()).isEqualTo(2);
    assertThat(
            result.stream().filter(s -> s.getTable().equals("tab1")).findFirst().get().getStatus())
        .isEqualTo("s2");
    assertThat(
            result.stream().filter(s -> s.getTable().equals("tab2")).findFirst().get().getStatus())
        .isEqualTo("s3");
  }

  @Test
  void run_noProjectId_throwsException() {
    StatusOptions options = new StatusOptions(new String[] {"--database", "db1"});
    LoggingClient mockClient = mock(LoggingClient.class);

    assertThrows(
        InvalidArgumentException.class,
        () ->
            new ListStatusForDatabase(
                options, () -> mockClient, ignored -> {}, new MessageTranslator()));
  }

  @Test
  void run_noDatabase_throwsException() {
    StatusOptions options = new StatusOptions(new String[] {"--project-id", "project1"});
    LoggingClient mockClient = mock(LoggingClient.class);

    assertThrows(
        InvalidArgumentException.class,
        () ->
            new ListStatusForDatabase(
                options, () -> mockClient, ignored -> {}, new MessageTranslator()));
  }

  private static LogEntry logMessage(String table, String status, long seconds) {
    return LogEntry.newBuilder()
        .setJsonPayload(
            Struct.newBuilder()
                .putFields(
                    "message",
                    Value.newBuilder()
                        .setStringValue(
                            String.format(
                                "table status: {\"source\":{\"database\": \"db1\", \"table\": \"%s\"}, \"status\": \"%s\"}",
                                table, status))
                        .build())
                .build())
        .setTimestamp(Timestamp.newBuilder().setSeconds(seconds).build())
        .build();
  }
}
