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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shevek
 */
public class ZonedIntervalIterable implements Iterable<ZonedInterval> {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(ZonedIntervalIterable.class);

  private final ZonedDateTime start;
  private final ZonedDateTime end;
  private final Duration duration;

  @Nonnull
  @VisibleForTesting
  /* pp */ static ZonedIntervalIterable forDateTimeRange(
      @Nonnull ZonedDateTime start, @Nonnull ZonedDateTime end, @Nonnull Duration duration) {
    return new ZonedIntervalIterable(start, end, duration);
  }

  @Nonnull
  @VisibleForTesting
  /* pp */ static ZonedIntervalIterable forTimeUnitsUntil(
      @Nonnull ZonedDateTime now, @Nonnegative int unitCount, @Nonnull Duration duration) {

    return forDateTimeRange(
        now.minus(Duration.ofSeconds(unitCount * duration.getSeconds())),
        now.plus(duration), // view comment in ZonedIntervalIterableTest, adding .minus(1,
        // ChronoUnit.MILLIS)
        // only fixes cases when now is padded at xx:00:00
        duration);
  }

  @Nonnull
  @VisibleForTesting
  /* pp */ static ZonedIntervalIterable forTimeUnitsUntilNow(
      @Nonnegative int unitCount, @Nonnull Duration duration) {
    return forTimeUnitsUntil(ZonedDateTime.now(ZoneOffset.UTC), unitCount, duration);
  }

  /**
   * Builds a ZonedIntervalIterable from connector arguments, the intervals will all be one hour
   * long ({@link ChronoUnit#HOURS}) and have and inclusive starting datetime and exclusive ending
   * datetime (i.e.: start <= t < end ).
   *
   * @param arguments connector arguments
   * @return a nonnull ZonedIntervalIterable
   * @throws MetadataDumperUsageException in case of arguments incompatibility or missing arguments
   */
  @Nonnull
  public static ZonedIntervalIterable forConnectorArguments(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    return ZonedIntervalIterable.forConnectorArguments(arguments, Duration.ofHours(1));
  }

  /**
   * Builds a ZonedIntervalIterable from connector arguments with the specified interval. The
   * intervals have inclusive starting datetime and exclusive ending datetime (i.e.: start <= t <
   * end ).
   *
   * @param arguments connector arguments
   * @param duration the length of the intervals
   * @return a nonnull ZonedIntervalIterable
   * @throws MetadataDumperUsageException in case of arguments incompatibility or missing arguments
   * @throws IllegalArgumentException if then {@link Duration} is more than a day or does not divide
   *     a day evenly
   */
  @Nonnull
  public static ZonedIntervalIterable forConnectorArguments(
      @Nonnull ConnectorArguments arguments, @Nonnull Duration duration)
      throws MetadataDumperUsageException {
    if (!isValidDuration(duration)) {
      throw new IllegalArgumentException(
          "Invalid `duration` provided. Please make sure the duration is less than a day and"
              + " divides a 24 hour period evenly.");
    }

    if (arguments.getQueryLogStart() != null || arguments.getQueryLogEnd() != null) {
      if (arguments.getQueryLogDays() != null)
        throw new MetadataDumperUsageException(
            "Incompatible options, either specify a number of log days to export or a start/end"
                + " timestamp.");

      if (arguments.getQueryLogStart() == null)
        throw new MetadataDumperUsageException(
            "Missing option --query-log-start must be specified.");
      if (arguments.getQueryLogEnd() == null)
        LOG.info(
            "Missing option --query-log-end will be defaulted to: "
                + arguments.getQueryLogEndOrDefault());

      LOG.info(
          "Log entries from {} to {} will be exported in increments of {} seconds.",
          arguments.getQueryLogStart(),
          arguments.getQueryLogEndOrDefault(),
          duration.getSeconds());
      return ZonedIntervalIterable.forDateTimeRange(
          arguments.getQueryLogStart(), arguments.getQueryLogEndOrDefault(), duration);
    }

    final int daysToExport = arguments.getQueryLogDays(7);
    if (daysToExport <= 0)
      throw new MetadataDumperUsageException(
          "At least one day of query logs should be exported; you specified: " + daysToExport);

    LOG.info(
        "Log entries within the last {} days will be exported in increments of {} seconds.",
        daysToExport,
        duration.getSeconds());

    int chunksInADay = Math.toIntExact(Duration.ofDays(1).getSeconds() / duration.getSeconds());
    return ZonedIntervalIterable.forTimeUnitsUntilNow(chunksInADay * daysToExport, duration);
  }

  private static boolean isValidDuration(Duration duration) {
    if (duration.isNegative() || duration.isZero()) {
      return false;
    }
    boolean atMostADay = duration.toDays() <= 1;
    boolean dividesADayEvenly = Duration.ofDays(1).getSeconds() % duration.getSeconds() == 0;
    return atMostADay && dividesADayEvenly;
  }

  private ZonedIntervalIterable(
      @Nonnull ZonedDateTime queryLogStart,
      @Nonnull ZonedDateTime queryLogEnd,
      @Nonnull Duration duration) {
    this.duration = Preconditions.checkNotNull(duration, "Duration was null.");
    Preconditions.checkNotNull(queryLogStart, "Query log start was null.");
    Preconditions.checkNotNull(queryLogEnd, "Query log end was null.");

    Preconditions.checkState(
        queryLogStart.isBefore(queryLogEnd),
        "Start date %s must precede end date %s",
        queryLogStart,
        queryLogEnd);

    this.start = queryLogStart;
    this.end = truncate(queryLogStart, queryLogEnd, duration);

    if (!end.equals(queryLogEnd)) {
      LOG.warn("End time has been truncated to {}", end);
    }
  }

  private ZonedDateTime truncate(
      @Nonnull ZonedDateTime queryLogStart,
      @Nonnull ZonedDateTime queryLogEnd,
      @Nonnull Duration duration) {
    long numberOfUnits =
        Math.floorDiv(
            Duration.between(queryLogStart, queryLogEnd).getSeconds(), duration.getSeconds());
    return queryLogStart.plus(Duration.ofSeconds(numberOfUnits * duration.getSeconds()));
  }

  @Nonnull
  public ZonedDateTime getStart() {
    return start;
  }

  @Nonnull
  public ZonedDateTime getEnd() {
    return this.end;
  }

  @Nonnull
  public Duration getDuration() {
    return duration;
  }

  private class Itr extends AbstractIterator<ZonedInterval> {

    private ZonedDateTime current;

    public Itr() {
      this.current = start;
    }

    @Override
    protected ZonedInterval computeNext() {
      if (current.isEqual(end) || current.isAfter(end)) return endOfData();

      ZonedDateTime next = current.plus(duration);
      if (next.isAfter(end)) next = end;
      ZonedInterval result = new ZonedInterval(current, next);

      current = next;
      return result;
    }
  }

  @Nonnull
  @Override
  public Iterator<ZonedInterval> iterator() {
    return new Itr();
  }

  @Override
  public String toString() {
    return String.format("from %s to %s every %s", start, end, duration);
  }
}
