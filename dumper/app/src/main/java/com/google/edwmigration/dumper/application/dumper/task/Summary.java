/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.util.Optional;
import java.util.function.BinaryOperator;

public class Summary {

  public static final BinaryOperator<Summary> COMBINER =
      (s1, s2) -> {
        if (s1.rowCount() == 0) return s2;
        if (s2.rowCount() == 0) return s1;

        long rowCount = s1.rowCount() + s2.rowCount();

        Optional<ZonedInterval> interval1 = s1.interval();
        Optional<ZonedInterval> interval2 = s2.interval();

        if (!interval1.isPresent()) return new Summary(rowCount, interval2);
        return interval2
            .map(interval1.get()::span)
            .map(span -> new Summary(rowCount, Optional.of(span)))
            .orElseGet(() -> new Summary(rowCount, interval1));
      };

  public static final Summary EMPTY = new Summary(0, Optional.empty());

  private final long rowCount;

  private final Optional<ZonedInterval> interval;

  /* pp */ Summary(long rowCount, Optional<ZonedInterval> interval) {
    this.rowCount = rowCount;
    this.interval = interval;
  }

  public long rowCount() {
    return rowCount;
  }

  public Optional<ZonedInterval> interval() {
    return interval;
  }
}
