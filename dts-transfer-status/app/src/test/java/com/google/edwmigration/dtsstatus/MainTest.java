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
package com.google.edwmigration.dtsstatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import com.google.edwmigration.dtsstatus.command.ListStatusForConfig;
import com.google.edwmigration.dtsstatus.command.ListStatusForDatabase;
import com.google.edwmigration.dtsstatus.command.ListTransferConfigs;
import com.google.edwmigration.dtsstatus.consumer.ConfigConsumerFactory;
import com.google.edwmigration.dtsstatus.consumer.StatusConsumerFactory;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class MainTest {

  @Test
  void main_listTransferConfigs_createdCorrectCommand() {
    try (MockedStatic<ConfigConsumerFactory> consumerFactory =
            mockStatic(ConfigConsumerFactory.class);
        MockedStatic<ListTransferConfigs> commandStatic = mockStatic(ListTransferConfigs.class)) {
      consumerFactory.when(() -> ConfigConsumerFactory.create(any())).thenReturn(null);
      ListTransferConfigs command = mock(ListTransferConfigs.class);
      commandStatic.when(() -> ListTransferConfigs.instance(any(), any())).thenReturn(command);

      Main.main(new String[] {"--list-transfer-configs"});

      commandStatic.verify(() -> ListTransferConfigs.instance(any(), any()), times(1));
    }
  }

  @Test
  void main_listStatusForConfig_createdCorrectCommand() {
    try (MockedStatic<StatusConsumerFactory> consumerFactory =
            mockStatic(StatusConsumerFactory.class);
        MockedStatic<ListStatusForConfig> commandStatic = mockStatic(ListStatusForConfig.class)) {
      consumerFactory.when(() -> StatusConsumerFactory.create(any())).thenReturn(null);
      ListStatusForConfig command = mock(ListStatusForConfig.class);
      commandStatic.when(() -> ListStatusForConfig.instance(any(), any())).thenReturn(command);

      Main.main(new String[] {"--list-status-for-config"});

      commandStatic.verify(() -> ListStatusForConfig.instance(any(), any()), times(1));
    }
  }

  @Test
  void main_listStatusForDatabase_createdCorrectCommand() {
    try (MockedStatic<StatusConsumerFactory> consumerFactory =
            mockStatic(StatusConsumerFactory.class);
        MockedStatic<ListStatusForDatabase> commandStatic =
            mockStatic(ListStatusForDatabase.class)) {
      consumerFactory.when(() -> StatusConsumerFactory.create(any())).thenReturn(null);
      ListStatusForDatabase command = mock(ListStatusForDatabase.class);
      commandStatic.when(() -> ListStatusForDatabase.instance(any(), any())).thenReturn(command);

      Main.main(new String[] {"--list-status-for-database"});

      commandStatic.verify(() -> ListStatusForDatabase.instance(any(), any()), times(1));
    }
  }

  @Test
  void main_multipleCommands_listConfigsCreated() {
    try (MockedStatic<ConfigConsumerFactory> consumerFactory =
            mockStatic(ConfigConsumerFactory.class);
        MockedStatic<ListTransferConfigs> transferConfigsCommandStatic =
            mockStatic(ListTransferConfigs.class);
        MockedStatic<ListStatusForConfig> statusForConfigCommandStatic =
            mockStatic(ListStatusForConfig.class);
        MockedStatic<ListStatusForDatabase> statusForDatabaseCommandStatic =
            mockStatic(ListStatusForDatabase.class)) {
      consumerFactory.when(() -> ConfigConsumerFactory.create(any())).thenReturn(null);
      ListTransferConfigs command = mock(ListTransferConfigs.class);
      transferConfigsCommandStatic
          .when(() -> ListTransferConfigs.instance(any(), any()))
          .thenReturn(command);

      Main.main(
          new String[] {
            "--list-transfer-configs", "--list-status-for-config", "--list-status-for-database"
          });

      transferConfigsCommandStatic.verify(
          () -> ListTransferConfigs.instance(any(), any()), times(1));
      statusForConfigCommandStatic.verify(
          () -> ListStatusForConfig.instance(any(), any()), times(0));
      statusForDatabaseCommandStatic.verify(
          () -> ListStatusForDatabase.instance(any(), any()), times(0));
    }
  }
}
