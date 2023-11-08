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
package com.google.edwmigration.dumper.application.dumper.connector;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Expands a given interval based on conditions. MUST enclose the original interval i.e. -
 * resultingInterval.getStart() <= originalInterval.getStart() && originalInterval.getEnd() <=
 * resultingInterval.getEnd()
 *
 * @author ishmum
 */
public interface IntervalExpander extends Function<ZonedInterval, ZonedInterval> {

  @Nonnull
  ZonedInterval apply(ZonedInterval interval);

  /**
   * Expands to match the duration. e.g. :
   *
   * <p>start time - 11 pm Saturday<br>
   * end time - 1 am Sunday<br>
   * rotation basis - daily<br>
   * actual dump - 2 files => all of Saturday + all of Sunday<br>
   *
   * <p>start time - 11 pm Saturday<br>
   * end time - 1 am Sunday<br>
   * rotation basis - hourly<br>
   * actual dump - 2 files => (11 pm -> 12 am) of Saturday + (12 am -> 1 am) of Sunday<br>
   */
  static IntervalExpander createBasedOnDuration(Duration duration) {
    return new IntervalExpander() {
      @Nonnull
      @Override
      public ZonedInterval apply(ZonedInterval interval) {
        ChronoUnit chronoUnit = convert(duration);
        ZonedDateTime endTime = interval.getEndInclusive().truncatedTo(chronoUnit);
        endTime = endTime.equals(interval.getEndInclusive()) ? endTime : endTime.plus(duration);
        return new ZonedInterval(interval.getStart().truncatedTo(chronoUnit), endTime);
      }

      private ChronoUnit convert(Duration duration) {
        return Stream.of(ChronoUnit.HOURS, ChronoUnit.DAYS)
            .filter(unit -> duration.equals(unit.getDuration()))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unsupported duration: " + duration));
      }
    };
  }
}
