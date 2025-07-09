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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListTransferLogsPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListTransferRunsPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.TransferConfigName;
import com.google.cloud.bigquery.datatransfer.v1.TransferMessage;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.edwmigration.dtsstatus.util.MessageTranslator;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListStatusForConfigTest {

  @Test
  void run_multipleRuns_listsStatusesCorrectly() {
    StatusOptions options =
        new StatusOptions(
            new String[] {
              "--project-id", "project1",
              "--location", "region1",
              "--config-id", "config1"
            });
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    TransferConfigName transferConfigName =
        TransferConfigName.ofProjectLocationTransferConfigName("project1", "region1", "config1");
    ListTransferRunsPagedResponse transferRunsResponse = mock(ListTransferRunsPagedResponse.class);
    when(transferRunsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                com.google.cloud.bigquery.datatransfer.v1.TransferRun.newBuilder()
                    .setName("run1")
                    .build(),
                com.google.cloud.bigquery.datatransfer.v1.TransferRun.newBuilder()
                    .setName("run2")
                    .build()));
    when(mockClient.listTransferRuns(transferConfigName)).thenReturn(transferRunsResponse);

    ListTransferLogsPagedResponse logsResponse1 = mock(ListTransferLogsPagedResponse.class);
    when(logsResponse1.iterateAll())
        .thenReturn(Arrays.asList(transferMessage("db1", "tab1", "s1", 1)));
    ListTransferLogsPagedResponse logsResponse2 = mock(ListTransferLogsPagedResponse.class);
    when(logsResponse2.iterateAll())
        .thenReturn(Arrays.asList(transferMessage("db1", "tab1", "s2", 2)));
    when(mockClient.listTransferLogs(anyString())).thenReturn(logsResponse1, logsResponse2);
    List<TransferStatus> result = new ArrayList<>();

    ListStatusForConfig command =
        new ListStatusForConfig(options, () -> mockClient, result::add, new MessageTranslator());

    command.run();

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getStatus()).isEqualTo("s2");
  }

  @Test
  void run_groupsDatabasesAndTablesCorrectly() {
    StatusOptions options =
        new StatusOptions(
            new String[] {
              "--project-id", "project1",
              "--location", "region1",
              "--config-id", "config1"
            });
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    TransferConfigName transferConfigName =
        TransferConfigName.ofProjectLocationTransferConfigName("project1", "region1", "config1");
    ListTransferRunsPagedResponse transferRunsResponse = mock(ListTransferRunsPagedResponse.class);
    when(transferRunsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                com.google.cloud.bigquery.datatransfer.v1.TransferRun.newBuilder()
                    .setName("run1")
                    .build()));
    when(mockClient.listTransferRuns(transferConfigName)).thenReturn(transferRunsResponse);

    ListTransferLogsPagedResponse logsResponse1 = mock(ListTransferLogsPagedResponse.class);
    when(logsResponse1.iterateAll())
        .thenReturn(
            Arrays.asList(
                transferMessage("db1", "tab1", "s1", 1),
                transferMessage("db1", "tab1", "s2", 2),
                transferMessage("db1", "tab2", "s3", 4),
                transferMessage("db1", "tab2", "s1", 3),
                transferMessage("db2", "tab1", "s1", 5),
                transferMessage("db2", "tab1", "s4", 6)));
    when(mockClient.listTransferLogs(anyString())).thenReturn(logsResponse1);
    List<TransferStatus> result = new ArrayList<>();

    ListStatusForConfig command =
        new ListStatusForConfig(options, () -> mockClient, result::add, new MessageTranslator());

    command.run();

    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).getStatus()).isEqualTo("s2");
    assertThat(result.get(1).getStatus()).isEqualTo("s3");
    assertThat(result.get(2).getStatus()).isEqualTo("s4");
  }

  @Test
  void run_filtersOutNonStatusLogs() {
    StatusOptions options =
        new StatusOptions(
            new String[] {
              "--project-id", "project1",
              "--location", "region1",
              "--config-id", "config1"
            });
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    TransferConfigName transferConfigName =
        TransferConfigName.ofProjectLocationTransferConfigName("project1", "region1", "config1");
    ListTransferRunsPagedResponse transferRunsResponse = mock(ListTransferRunsPagedResponse.class);
    when(transferRunsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                com.google.cloud.bigquery.datatransfer.v1.TransferRun.newBuilder()
                    .setName("run1")
                    .build()));
    when(mockClient.listTransferRuns(transferConfigName)).thenReturn(transferRunsResponse);

    ListTransferLogsPagedResponse logsResponse = mock(ListTransferLogsPagedResponse.class);
    when(logsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                TransferMessage.newBuilder()
                    .setMessageText("not a status message")
                    .setMessageTime(Timestamp.newBuilder().setSeconds(3).build())
                    .build(),
                transferMessage("db1", "tab1", "s1", 1),
                TransferMessage.newBuilder()
                    .setMessageText("not a status message")
                    .setMessageTime(Timestamp.newBuilder().setSeconds(2).build())
                    .build()));
    when(mockClient.listTransferLogs(anyString())).thenReturn(logsResponse);
    List<TransferStatus> result = new ArrayList<>();

    ListStatusForConfig command =
        new ListStatusForConfig(options, () -> mockClient, result::add, new MessageTranslator());

    command.run();

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getStatus()).isEqualTo("s1");
  }

  @Test
  void run_ignoresMalformedStatusLogs() {
    StatusOptions options =
        new StatusOptions(
            new String[] {
              "--project-id", "project1",
              "--location", "region1",
              "--config-id", "config1"
            });
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    TransferConfigName transferConfigName =
        TransferConfigName.ofProjectLocationTransferConfigName("project1", "region1", "config1");
    ListTransferRunsPagedResponse transferRunsResponse = mock(ListTransferRunsPagedResponse.class);
    when(transferRunsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                com.google.cloud.bigquery.datatransfer.v1.TransferRun.newBuilder()
                    .setName("run1")
                    .build()));
    when(mockClient.listTransferRuns(transferConfigName)).thenReturn(transferRunsResponse);

    ListTransferLogsPagedResponse logsResponse = mock(ListTransferLogsPagedResponse.class);
    when(logsResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                transferMessage("db1", "tab1", "s1", 1),
                // message missing the table field
                TransferMessage.newBuilder()
                    .setMessageText(
                        "table status: {\"source\":{\"database\": \"db1\"}, \"status\": \"s2\"}")
                    .setMessageTime(Timestamp.newBuilder().setSeconds(2).build())
                    .build(),
                // message with changed source field name
                TransferMessage.newBuilder()
                    .setMessageText(
                        "table status: {\"src\":{\"database\": \"db1\", \"table\": \"tab1\"}, \"status\": \"s2\"}")
                    .setMessageTime(Timestamp.newBuilder().setSeconds(3).build())
                    .build()));
    when(mockClient.listTransferLogs(anyString())).thenReturn(logsResponse);
    List<TransferStatus> result = new ArrayList<>();

    ListStatusForConfig command =
        new ListStatusForConfig(options, () -> mockClient, result::add, new MessageTranslator());

    command.run();

    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).getStatus()).isEqualTo("s1");
  }

  @Test
  void constructor_noProjectId_throwsException() {
    StatusOptions options =
        new StatusOptions(new String[] {"--location", "region1", "--config-id", "config1"});
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);

    assertThrows(
        InvalidArgumentException.class,
        () ->
            new ListStatusForConfig(
                options, () -> mockClient, ignored -> {}, new MessageTranslator()));
  }

  @Test
  void constructor_noLocation_throwsException() {
    StatusOptions options =
        new StatusOptions(new String[] {"--project-id", "project1", "--config-id", "config1"});
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);

    assertThrows(
        InvalidArgumentException.class,
        () ->
            new ListStatusForConfig(
                options, () -> mockClient, ignored -> {}, new MessageTranslator()));
  }

  @Test
  void constructor_noConfigId_throwsException() {
    StatusOptions options =
        new StatusOptions(
            new String[] {
              "--project-id", "project1", "--location", "region1",
            });
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);

    assertThrows(
        InvalidArgumentException.class,
        () ->
            new ListStatusForConfig(
                options, () -> mockClient, ignored -> {}, new MessageTranslator()));
  }

  private static TransferMessage transferMessage(
      String database, String table, String status, long seconds) {
    return TransferMessage.newBuilder()
        .setMessageText(
            String.format(
                "table status: {\"source\":{\"database\": \"%s\", \"table\": \"%s\"}, \"status\": \"%s\"}",
                database, table, status))
        .setMessageTime(Timestamp.newBuilder().setSeconds(seconds).build())
        .build();
  }
}
