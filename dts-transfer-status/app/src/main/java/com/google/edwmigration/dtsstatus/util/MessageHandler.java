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

import static java.util.stream.Collectors.groupingBy;

import com.google.edwmigration.dtsstatus.model.TransferStatus;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class MessageHandler {

  public static void consumeLatestExistingStatuses(
      Stream<Optional<TransferStatus>> statuses, Consumer<TransferStatus> statusConsumer) {
    statuses
        // filter out missing statuses
        .filter(Optional::isPresent)
        .map(Optional::get)
        // group by database and table and return the latest status for each group
        .collect(groupingBy(TransferStatus::getDatabase))
        .values()
        .stream()
        .flatMap(
            dbGroup ->
                dbGroup.stream().collect(groupingBy(TransferStatus::getTable)).values().stream())
        .map(Collections::max)
        .sorted()
        .forEach(statusConsumer);
  }
}
