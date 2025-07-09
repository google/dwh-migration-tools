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

import static com.google.common.truth.Truth.assertThat;

import com.google.edwmigration.dtsstatus.model.TransferStatus;
import com.google.protobuf.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class MessageHandlerTest {

  @Test
  void consumeLatestExistingStatuses_consumesTheLatestStatus() {
    List<TransferStatus> consumedStatuses = new ArrayList<>();
    Stream<Optional<TransferStatus>> statuses =
        Stream.of(
            Optional.of(transferStatus("db1", "table1", "status1", 1)),
            Optional.of(transferStatus("db1", "table1", "status3", 2)),
            Optional.of(transferStatus("db1", "table2", "status2", 3)));
    MessageHandler.consumeLatestExistingStatuses(statuses, consumedStatuses::add);
    assertThat(consumedStatuses.size()).isEqualTo(2);
    assertThat(
            consumedStatuses.stream()
                .filter(s -> s.getTable().equals("table1"))
                .findFirst()
                .get()
                .getStatus())
        .isEqualTo("status3");
  }

  @Test
  void consumeLatestExistingStatuses_filtersOutEmptyStatues() {
    List<TransferStatus> consumedStatuses = new ArrayList<>();
    Stream<Optional<TransferStatus>> statuses =
        Stream.of(
            Optional.empty(),
            Optional.of(transferStatus("db1", "table1", "status1", 1)),
            Optional.empty());
    MessageHandler.consumeLatestExistingStatuses(statuses, consumedStatuses::add);
    assertThat(consumedStatuses.size()).isEqualTo(1);
    assertThat(consumedStatuses.get(0).getStatus()).isEqualTo("status1");
  }

  @Test
  void consumeLatestExistingStatuses_groupsTableStatuses() {
    List<TransferStatus> consumedStatuses = new ArrayList<>();
    Stream<Optional<TransferStatus>> statuses =
        Stream.of(
            Optional.of(transferStatus("db1", "table1", "status1", 1)),
            Optional.of(transferStatus("db1", "table1", "status2", 2)),
            Optional.of(transferStatus("db1", "table2", "status3", 3)),
            Optional.of(transferStatus("db2", "table2", "status4", 3)));
    MessageHandler.consumeLatestExistingStatuses(statuses, consumedStatuses::add);
    assertThat(consumedStatuses.size()).isEqualTo(3);
  }

  @Test
  void consumeLatestExistingStatuses_sortsStatuses() {
    List<TransferStatus> consumedStatuses = new ArrayList<>();
    Stream<Optional<TransferStatus>> statuses =
        Stream.of(
            Optional.of(transferStatus("db2", "table2", "status1", 1)),
            Optional.of(transferStatus("db1", "table1", "status2", 2)),
            Optional.of(transferStatus("db1", "table2", "status3", 3)),
            Optional.of(transferStatus("db2", "table1", "status4", 3)));
    MessageHandler.consumeLatestExistingStatuses(statuses, consumedStatuses::add);

    assertThat(consumedStatuses).isInOrder(Comparator.naturalOrder());
  }

  private static TransferStatus transferStatus(
      String database, String table, String status, long seconds) {
    return new TransferStatus(
        database, table, status, Timestamp.newBuilder().setSeconds(seconds).build());
  }
}
