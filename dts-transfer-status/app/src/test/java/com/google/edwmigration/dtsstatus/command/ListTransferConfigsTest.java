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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient;
import com.google.cloud.bigquery.datatransfer.v1.DataTransferServiceClient.ListTransferConfigsPagedResponse;
import com.google.cloud.bigquery.datatransfer.v1.LocationName;
import com.google.edwmigration.dtsstatus.StatusOptions;
import com.google.edwmigration.dtsstatus.exception.InvalidArgumentException;
import com.google.edwmigration.dtsstatus.model.TransferConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ListTransferConfigsTest {
  @Test
  void run_providesConfigs() {
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    ListTransferConfigsPagedResponse mockResponse = mock(ListTransferConfigsPagedResponse.class);
    when(mockResponse.iterateAll())
        .thenReturn(
            Arrays.asList(
                com.google.cloud.bigquery.datatransfer.v1.TransferConfig.newBuilder()
                    .setName("a/b/c/d/e/config1-id")
                    .setDisplayName("config1")
                    .build(),
                com.google.cloud.bigquery.datatransfer.v1.TransferConfig.newBuilder()
                    .setName("a/b/c/d/e/config2-id")
                    .setDisplayName("config2")
                    .build()));
    LocationName locationName = LocationName.of("project1", "region1");
    when(mockClient.listTransferConfigs(locationName)).thenReturn(mockResponse);
    StatusOptions options =
        new StatusOptions(new String[] {"--project-id", "project1", "--location", "region1"});
    List<TransferConfig> result = new ArrayList<>();

    ListTransferConfigs command = new ListTransferConfigs(options, () -> mockClient, result::add);

    command.run();

    assertThat(result.size()).isEqualTo(2);
    assertThat(result.get(0).getConfigId()).isEqualTo("config1-id");
    assertThat(result.get(0).getName()).isEqualTo("config1");
  }

  @Test
  void constructor_noProjectId_throwsException() {
    DataTransferServiceClient mockClient = mock(DataTransferServiceClient.class);
    StatusOptions options = new StatusOptions(new String[0]);
    assertThrows(
        InvalidArgumentException.class,
        () -> new ListTransferConfigs(options, () -> mockClient, ignored -> {}));
  }
}
