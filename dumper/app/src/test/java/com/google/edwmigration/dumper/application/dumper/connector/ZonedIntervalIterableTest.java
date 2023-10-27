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

import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class ZonedIntervalIterableTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(ZonedIntervalIterableTest.class);

  private static final ConnectorArguments.ZonedParser zonedParserStart =
      new ConnectorArguments.ZonedParser(
          ConnectorArguments.ZonedParser.DEFAULT_PATTERN,
          ConnectorArguments.ZonedParser.DayOffset.START_OF_DAY);
  private static final ConnectorArguments.ZonedParser zonedParserEnd =
      new ConnectorArguments.ZonedParser(
          ConnectorArguments.ZonedParser.DEFAULT_PATTERN,
          ConnectorArguments.ZonedParser.DayOffset.END_OF_DAY);

  private void testIterable(int expectCount, @Nonnegative ZonedIntervalIterable iterable) {
    LOG.debug("Testing {}", iterable);
    int actualCount = 0;
    for (ZonedInterval interval : iterable) {
      LOG.debug("Interval is {}", interval);
      assertEquals(iterable.getDuration(), interval.getDuration());
      actualCount++;
    }
    assertEquals(expectCount, actualCount);
  }

  // I'm not entirely sure with the behaviour of this over time-zone shifts:
  // Interval is
  // 2019-11-02T00:00-07:00[America/Los_Angeles]...2019-11-02T23:59:59.999-07:00[America/Los_Angeles]
  // Interval is
  // 2019-11-03T00:00-07:00[America/Los_Angeles]...2019-11-03T23:59:59.999-08:00[America/Los_Angeles]
  // Interval is
  // 2019-11-04T00:00-08:00[America/Los_Angeles]...2019-11-04T23:59:59.999-08:00[America/Los_Angeles]
  // Interval is
  // 2019-11-05T00:00-08:00[America/Los_Angeles]...2019-11-05T23:59:59.999-08:00[America/Los_Angeles]

  private void assertIterations(
      int expected, @Nonnull String from, @Nonnull String to, ChronoUnit chronoUnit) {
    Duration duration = chronoUnit.getDuration();
    ZonedIntervalIterable iterable =
        new ZonedIntervalIterable(
            zonedParserStart.convert(from),
            zonedParserEnd.convert(to),
            duration,
            TimeTruncator.createBasedOnDuration(duration));
    testIterable(expected, iterable);
  }

  @Test
  public void testHours() {
    Duration durationOfDay = Duration.ofDays(1);
    TimeTruncator dayTruncator = TimeTruncator.createBasedOnDuration(durationOfDay);

    Duration durationOfHour = Duration.ofDays(1);
    TimeTruncator hourTruncator = TimeTruncator.createBasedOnDuration(durationOfDay);

    testIterable(
        170,
        ZonedIntervalIterableGenerator.forTimeUnitsUntilNow(24 * 7, durationOfDay, dayTruncator));
    testIterable(
        9, ZonedIntervalIterableGenerator.forTimeUnitsUntilNow(7, durationOfDay, dayTruncator));

    testIterable(
        2,
        ZonedIntervalIterableGenerator.forTimeUnitsUntil(
            ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")),
            1,
            durationOfHour,
            hourTruncator));
    // Testing from 2019-12-31T23:00Z[UTC] to 2020-01-01T01:00Z[UTC] every Hours
    // Interval is 2019-12-31T23:00Z[UTC]...2019-12-31T23:59:59.999Z[UTC]
    // Interval is 2020-01-01T00:00Z[UTC]...2020-01-01T00:59:59.999Z[UTC]
    // I'm not convinced about this, if I ask the last 1 hours how come I get two hours of logs?
    //
    // The reason behind this is that we want "hourly aligned" log files, which is cool, but this
    // will - in my opinion - lead to double counting almost anytime, if I dump last 1 day every
    // midnight
    // I will get 24 full hours + 1 partial hour, the partial hour will be included in the first
    // slice of
    // next dump, assuming nightly dumps at midnight + some seconds/minutes
    int daysToExport = 1;
    testIterable(
        26,
        ZonedIntervalIterableGenerator.forTimeUnitsUntilNow(
            24 * daysToExport, durationOfHour, hourTruncator));
  }

  @Test
  public void testBetweenDatesIterable() {
    assertIterations(1, "2020-01-01 12:00:00", "2020-01-01 13:00:00", ChronoUnit.HOURS);
    assertIterations(2, "2020-01-01 11:00:00", "2020-01-01 13:00:00", ChronoUnit.HOURS);
    assertIterations(24, "2020-01-01 12:00:00", "2020-01-02 12:00:00", ChronoUnit.HOURS);

    assertIterations(24, "2020-01-01 12:00:00", "2020-01-02 12:00:00.000", ChronoUnit.HOURS);
    assertIterations(24, "2020-01-01", "2020-01-01", ChronoUnit.HOURS);
    assertIterations(48, "2020-01-01", "2020-01-02", ChronoUnit.HOURS);

    assertIterations(12, "2020-01-01 12:00:00", "2020-01-01", ChronoUnit.HOURS);
    assertIterations(36, "2020-01-01", "2020-01-02 12:00:00", ChronoUnit.HOURS);

    // Log warning on truncation
    assertIterations(37, "2020-01-01", "2020-01-02 12:15:00.999", ChronoUnit.HOURS);
    assertIterations(37, "2020-01-01 00:12:04.500", "2020-01-02 12:15:00.999", ChronoUnit.HOURS);
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidBetweenDates() {
    assertIterations(1 /* any */, "2020-01-01 12:00:00", "2020-01-01 11:00:00", ChronoUnit.HOURS);
  }

  @Test
  public void testForLogStartAndLogEnd() throws Throwable {
    LocalDate fourDaysAgo = LocalDate.now().minusDays(4);

    int daysExpected = 2;
    LocalDate requestedStart = fourDaysAgo;
    LocalDate requestedEnd = requestedStart.plusDays(daysExpected);
    ZonedDateTime expectedStart = requestedStart.atStartOfDay(ZoneOffset.UTC);
    ZonedDateTime expectedEnd = requestedEnd.plusDays(1).atStartOfDay(ZoneOffset.UTC);
    ConnectorArguments arguments =
        new ConnectorArguments(
            new String[] {
              "--query-log-start",
              requestedStart.toString(),
              "--query-log-end",
              requestedEnd.toString(),
              "--connector",
              "foobar"
            });
    checkIntervalForArguments(expectedStart, expectedEnd, arguments);
  }

  @Test
  public void testForLogStartTimeAndLogEndTime() throws Throwable {
    String requestedStart = "2020-06-30 09:17:00";
    String requestedEnd = "2020-07-05 10:45:00";
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern(ConnectorArguments.ZonedParser.DEFAULT_PATTERN, Locale.US);

    LocalDateTime requestedStartParsed = LocalDateTime.parse(requestedStart, formatter);
    LocalDateTime expectedEndParsed = LocalDateTime.parse("2020-07-05 10:17:00", formatter);

    ZonedDateTime expectedStart =
        requestedStartParsed.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS);
    ZonedDateTime expectedEnd =
        expectedEndParsed.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).plusHours(1);

    ConnectorArguments arguments =
        new ConnectorArguments(
            new String[] {
              "--query-log-start",
              requestedStart,
              "--query-log-end",
              requestedEnd,
              "--connector",
              "foobar"
            });
    checkIntervalForArguments(expectedStart, expectedEnd, arguments);
  }

  @Test
  public void testForLogDays() throws Throwable {
    int daysExpected = 5;
    ZonedDateTime nowAtUTC = ZonedDateTime.now(ZoneOffset.UTC);
    ZonedDateTime expectedStart = nowAtUTC.minusDays(daysExpected).truncatedTo(ChronoUnit.HOURS);
    ZonedDateTime expectedEnd =
        nowAtUTC.truncatedTo(ChronoUnit.HOURS).plusHours(2).truncatedTo(ChronoUnit.HOURS);
    ConnectorArguments arguments =
        new ConnectorArguments(
            new String[] {"--query-log-days", "" + daysExpected, "--connector", "foobar"});

    checkIntervalForArguments(expectedStart, expectedEnd, arguments);
  }

  @Test
  public void testForDefaultLogDays() throws Throwable {
    int daysExpected = 7;
    ZonedDateTime nowAtUTC = ZonedDateTime.now(ZoneOffset.UTC);
    ZonedDateTime expectedStart = nowAtUTC.minusDays(daysExpected).truncatedTo(ChronoUnit.HOURS);
    ZonedDateTime expectedEnd = nowAtUTC.plusHours(2).truncatedTo(ChronoUnit.HOURS);
    ConnectorArguments arguments = new ConnectorArguments(new String[] {"--connector", "foobar"});

    checkIntervalForArguments(expectedStart, expectedEnd, arguments);
  }

  @Test
  public void testTimeTruncator_HourBased() {
    ZonedDateTime startTime = ZonedDateTime.parse("2007-12-03T10:15:30+00:00");
    ZonedDateTime endTime = ZonedDateTime.parse("2007-12-03T13:01:30+00:00");
    ZonedInterval interval = new ZonedInterval(startTime, endTime);

    ZonedDateTime expandedStartTime = ZonedDateTime.parse("2007-12-03T10:00:00+00:00");
    ZonedDateTime expandedEndTime = ZonedDateTime.parse("2007-12-03T14:00:00+00:00");
    ZonedInterval expandedInterval = new ZonedInterval(expandedStartTime, expandedEndTime);

    // Act
    ZonedInterval result = TimeTruncator.createBasedOnDuration(Duration.ofHours(1)).apply(interval);

    // Assert
    assertEquals(expandedInterval, result);
  }

  @Test
  public void testTimeTruncator_DayBased() {
    ZonedDateTime startTime = ZonedDateTime.parse("2007-12-03T10:15:30+00:00");
    ZonedDateTime endTime = ZonedDateTime.parse("2007-12-03T13:01:30+00:00");
    ZonedInterval interval = new ZonedInterval(startTime, endTime);

    ZonedDateTime expandedStartTime = ZonedDateTime.parse("2007-12-03T00:00:00+00:00");
    ZonedDateTime expandedEndTime = ZonedDateTime.parse("2007-12-04T00:00:00+00:00");
    ZonedInterval expandedInterval = new ZonedInterval(expandedStartTime, expandedEndTime);

    // Act
    ZonedInterval result = TimeTruncator.createBasedOnDuration(Duration.ofDays(1)).apply(interval);

    // Assert
    assertEquals(expandedInterval, result);
  }

  private void checkIntervalForArguments(
      ZonedDateTime expectedStart, ZonedDateTime expectedEnd, ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    ZonedIntervalIterable interval =
        ZonedIntervalIterableGenerator.forConnectorArguments(arguments);
    assertEquals("Wrong start time", expectedStart, interval.getStart());
    assertEquals("Wrong end time", expectedEnd, interval.getEnd());
  }
}
