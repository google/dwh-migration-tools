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

import static java.time.ZoneOffset.UTC;

import com.google.common.base.Preconditions;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A zoned interval represents an interval of time delimited by an inclusive start zoned datetime
 * and an exclusive stop zoned datetime.
 *
 * @author shevek
 */
public class ZonedInterval {

  private final ZonedDateTime start;
  private final ZonedDateTime endExclusive;

  public ZonedInterval(ZonedDateTime start, ZonedDateTime endExclusive) {
    Preconditions.checkArgument(start.isBefore(endExclusive), "Start date must be before end date");
    this.start = start;
    this.endExclusive = endExclusive;
  }

  @Nonnull
  public ZonedDateTime getStart() {
    return start;
  }

  @Nonnull
  public ZonedDateTime getStartUTC() {
    return getStart().withZoneSameInstant(UTC);
  }

  @Nonnull
  public ZonedDateTime getEndExclusive() {
    return endExclusive;
  }

  @Nonnull
  public ZonedDateTime getEndExclusiveUTC() {
    return getEndExclusive().withZoneSameInstant(UTC);
  }

  @Nonnull
  public ZonedDateTime getEndInclusive() {
    return getEndExclusive().minus(1, ChronoUnit.MILLIS);
  }

  @Nonnull
  public ZonedDateTime getEndInclusiveUTC() {
    return getEndInclusive().withZoneSameInstant(UTC);
  }

  @Nonnull
  public ZonedInterval span(@Nonnull ZonedInterval interval) {
    ZonedDateTime startTime = ObjectUtils.min(interval.getStart(), this.getStart());
    ZonedDateTime endTime = ObjectUtils.max(interval.getEndExclusive(), this.getEndExclusive());
    return new ZonedInterval(startTime, endTime);
  }

  @Override
  public String toString() {
    return "[" + getStart() + ".." + getEndExclusive() + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ZonedInterval)) return false;
    ZonedInterval otherInterval = (ZonedInterval) obj;
    return otherInterval.start.isEqual(start) && otherInterval.endExclusive.isEqual(endExclusive);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(start.hashCode()).append(endExclusive.hashCode()).build();
  }
}
