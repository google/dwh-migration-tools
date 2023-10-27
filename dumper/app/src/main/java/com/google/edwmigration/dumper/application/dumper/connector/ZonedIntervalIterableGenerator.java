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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author ishmum */
public class ZonedIntervalIterableGenerator {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(ZonedIntervalIterableGenerator.class);

  @Nonnull
  @VisibleForTesting
  /* pp */ static ZonedIntervalIterable forTimeUnitsUntil(
      @Nonnull ZonedDateTime now,
      @Nonnegative long unitCount,
      @Nonnull Duration duration,
      @Nonnull TimeTruncator truncator) {
    return createZonedIntervals(
        now.minus(Duration.ofSeconds(unitCount * duration.getSeconds())),
        now.plus(duration),
        duration,
        truncator);
  }

  /** Returns an Iterable after truncating current time with `TimeTruncator` */
  @Nonnull
  @VisibleForTesting
  /* pp */ static ZonedIntervalIterable forTimeUnitsUntilNow(
      @Nonnegative long unitCount, @Nonnull Duration duration, @Nonnull TimeTruncator truncator) {
    return forTimeUnitsUntil(ZonedDateTime.now(ZoneOffset.UTC), unitCount, duration, truncator);
  }

  /**
   * Builds a ZonedIntervalIterable from connector arguments, the intervals will all be one hour
   * long ({@link ChronoUnit#HOURS}) and have an inclusive starting datetime and exclusive ending
   * datetime (i.e.: start <= t < end ).
   *
   * @param arguments connector arguments
   * @return a nonnull ZonedIntervalIterable
   * @throws MetadataDumperUsageException in case of arguments incompatibility or missing arguments
   */
  @Nonnull
  public static ZonedIntervalIterable forConnectorArguments(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    Duration duration = Duration.ofHours(1);
    return ZonedIntervalIterableGenerator.forConnectorArguments(
        arguments, duration, TimeTruncator.createBasedOnDuration(duration));
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
      @Nonnull ConnectorArguments arguments,
      @Nonnull Duration duration,
      @Nonnull TimeTruncator truncator)
      throws MetadataDumperUsageException {
    Preconditions.checkArgument(
        isValidDuration(duration),
        "Invalid duration provided. Please make sure the duration is less than a day and"
            + " divides a 24 hour period evenly.");

    if (arguments.getQueryLogStart() != null || arguments.getQueryLogEnd() != null) {
      if (arguments.getQueryLogDays() != null) {
        throw new MetadataDumperUsageException(
            "Incompatible options, either specify a number of log days to export or a start/end"
                + " timestamp.");
      }

      if (arguments.getQueryLogStart() == null) {
        throw new MetadataDumperUsageException(
            "Missing option --query-log-start must be specified.");
      }
      if (arguments.getQueryLogEnd() == null) {
        LOG.info(
            "Missing option --query-log-end will be defaulted to: "
                + arguments.getQueryLogEndOrDefault());
      }

      LOG.info(
          "Log entries from {} to {} will be exported in increments of {}.",
          arguments.getQueryLogStart(),
          arguments.getQueryLogEndOrDefault(),
          DurationFormatUtils.formatDurationWords(duration.toMillis(), true, true));
      return createZonedIntervals(
          arguments.getQueryLogStart(), arguments.getQueryLogEndOrDefault(), duration, truncator);
    }

    final int daysToExport = arguments.getQueryLogDays(7);
    if (daysToExport <= 0)
      throw new MetadataDumperUsageException(
          "At least one day of query logs should be exported; you specified: " + daysToExport);

    LOG.info(
        "Log entries within the last {} days will be exported in increments of {}.",
        daysToExport,
        DurationFormatUtils.formatDurationWords(duration.toMillis(), true, true));

    long chunksInADay = Duration.ofDays(1).getSeconds() / duration.getSeconds();
    return forTimeUnitsUntilNow(chunksInADay * daysToExport, duration, truncator);
  }

  private static ZonedIntervalIterable createZonedIntervals(
      ZonedDateTime start, ZonedDateTime end, Duration duration, TimeTruncator truncator) {
    return new ZonedIntervalIterable(start, end, duration, truncator);
  }

  private static boolean isValidDuration(Duration duration) {
    if (duration.isNegative() || duration.isZero()) {
      return false;
    }
    boolean atMostADay = duration.toDays() <= 1;
    boolean dividesADayEvenly = Duration.ofDays(1).getSeconds() % duration.getSeconds() == 0;
    return atMostADay && dividesADayEvenly;
  }
}
