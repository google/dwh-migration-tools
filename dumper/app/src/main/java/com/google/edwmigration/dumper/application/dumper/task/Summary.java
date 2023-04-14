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
import javax.annotation.Nonnull;

public class Summary {

  /*
   * This merges overlapping intervals without accounting for duplication.
   * No connector currently executes queries in overlapping intervals,
   * which could have resulted in duplicate rows.
   * The only case where we would merge two overlapping intervals is when
   * other threads have merged intervals previously having gaps between them.
   * This would later on be filled up by the overlap
   * */
  public static final BinaryOperator<Summary> COMBINER =
      (s1, s2) -> {
        if (s1.rowCount() == 0) return s2;
        if (s2.rowCount() == 0) return s1;

        long rowCount = s1.rowCount() + s2.rowCount();

        Optional<ZonedInterval> interval1 = s1.interval();
        Optional<ZonedInterval> interval2 = s2.interval();

        Summary summary = new Summary(rowCount);
        return interval1
            .map(
                interval ->
                    interval2
                        .map(interval::span)
                        .map(summary::withInterval)
                        .orElseGet(() -> summary.withInterval(interval)))
            .orElseGet(() -> interval2.map(summary::withInterval).orElse(summary));
      };

  public static final Summary EMPTY = new Summary(0);

  private final long rowCount;

  private Optional<ZonedInterval> interval;

  /* pp */ Summary(long rowCount) {
    this.rowCount = rowCount;
    this.interval = Optional.empty();
  }

  public long rowCount() {
    return rowCount;
  }

  public Optional<ZonedInterval> interval() {
    return interval;
  }

  public Summary withInterval(@Nonnull ZonedInterval interval) {
    this.interval = Optional.of(interval);
    return this;
  }

  @Override
  public String toString() {
    return "Summary(" + rowCount() + ", " + interval() + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Summary)) return false;
    Summary summary = (Summary) obj;
    return summary.rowCount() == rowCount() && summary.interval().equals(interval());
  }
}
