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
package com.google.edwmigration.dumper.application.dumper.connector;

import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.getTimeSubtractingDays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZonedIntervalTest {

  private final ZonedDateTime SEVEN_DAYS_AGO = getTimeSubtractingDays(7);
  private final ZonedDateTime FIVE_DAYS_AGO = getTimeSubtractingDays(5);
  private final ZonedDateTime THREE_DAYS_AGO = getTimeSubtractingDays(3);
  private final ZonedDateTime ONE_DAY_AGO = getTimeSubtractingDays(1);
  private static final ZonedDateTime START_UTC =
      ZonedDateTime.of(2024, 3, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime END_UTC = START_UTC.plusDays(2);
  private static final ZoneOffset OTHER_ZONE = ZoneOffset.ofHours(2);

  @Test
  public void equals_trueWhenStartAndEndTimeAreEqual() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);
    ZonedInterval i2 = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);

    // Assert
    assertEquals("Intervals should be equal", i1, i2);
  }

  @Test
  public void equals_falseWhenStartTimeIsDifferent() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);
    ZonedInterval i2 = new ZonedInterval(FIVE_DAYS_AGO, THREE_DAYS_AGO);

    // Assert
    assertNotEquals("Intervals should not be equal", i1, i2);
  }

  @Test
  public void equals_falseWhenEndTimeIsDifferent() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);
    ZonedInterval i2 = new ZonedInterval(SEVEN_DAYS_AGO, THREE_DAYS_AGO);

    // Assert
    assertNotEquals("Intervals should not be equal", i1, i2);
  }

  @Test
  public void equals_trueWhenSameInstantsAreInDifferentZones() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(START_UTC, END_UTC);
    ZonedInterval i2 =
        new ZonedInterval(
            START_UTC.withZoneSameInstant(OTHER_ZONE), END_UTC.withZoneSameInstant(OTHER_ZONE));

    // Assert
    assertEquals("Intervals should be equal", i1, i2);
  }

  @Test
  public void hashCode_sameWhenStartAndEndTimeAreEqual() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(START_UTC, END_UTC);
    ZonedInterval i2 = new ZonedInterval(START_UTC, END_UTC);

    // Assert
    assertEquals("Hash codes should be equal", i1.hashCode(), i2.hashCode());
  }

  @Test
  public void hashCode_sameWhenSameInstantsAreInDifferentZones() {
    // Arrange
    ZonedInterval i1 = new ZonedInterval(START_UTC, END_UTC);
    ZonedInterval i2 =
        new ZonedInterval(
            START_UTC.withZoneSameInstant(OTHER_ZONE), END_UTC.withZoneSameInstant(OTHER_ZONE));

    // Assert
    assertEquals("Hash codes should be equal", i1.hashCode(), i2.hashCode());
  }

  @Test
  public void inclusiveEnd_isSmallerThanExclusiveEnd() {
    // Arrange
    ZonedInterval interval = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);

    // Assert
    assertTrue(
        "Inclusive time should be smaller",
        interval.getEndInclusive().isBefore(interval.getEndExclusive()));
  }

  @Test
  public void span_combinesDisjointInterval() {
    // Arrange
    ZonedInterval earliestInterval = new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO);
    ZonedInterval latestInterval = new ZonedInterval(THREE_DAYS_AGO, ONE_DAY_AGO);
    ZonedInterval expectedInterval = new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO);

    // Act
    ZonedInterval resultingInterval = earliestInterval.span(latestInterval);

    // Assert
    assertEquals(expectedInterval, resultingInterval);
  }

  @Test
  public void span_combinesOverlappingInterval() {
    // Arrange
    ZonedInterval earliestInterval = new ZonedInterval(SEVEN_DAYS_AGO, THREE_DAYS_AGO);
    ZonedInterval latestInterval = new ZonedInterval(FIVE_DAYS_AGO, ONE_DAY_AGO);
    ZonedInterval expectedInterval = new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO);

    // Act
    ZonedInterval resultingInterval = earliestInterval.span(latestInterval);

    // Assert
    assertEquals(expectedInterval, resultingInterval);
  }

  @Test
  public void span_combinesSubsetInterval() {
    // Arrange
    ZonedInterval earliestInterval = new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO);
    ZonedInterval latestInterval = new ZonedInterval(FIVE_DAYS_AGO, THREE_DAYS_AGO);
    ZonedInterval expectedInterval = new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO);

    // Act
    ZonedInterval resultingInterval = earliestInterval.span(latestInterval);

    // Assert
    assertEquals(expectedInterval, resultingInterval);
  }

  @Test
  public void intervalCreationValidation() {
    // Arrange & Act
    IllegalArgumentException exception =
        Assert.assertThrows(
            IllegalArgumentException.class, () -> new ZonedInterval(THREE_DAYS_AGO, FIVE_DAYS_AGO));

    // Assert
    assertEquals("Start date must be before end date", exception.getMessage());
  }
}
