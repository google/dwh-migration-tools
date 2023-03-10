/*
 * Copyright 2022 Google LLC
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
import static org.junit.Assert.assertTrue;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class ZonedIntervalTest {

    @SuppressWarnings("UnusedVariable")
    private static final Logger LOG = LoggerFactory.getLogger(ZonedIntervalTest.class);

    private final ZonedDateTime sevenDaysAgo = getTimeSubtractingDays(7);
    private final ZonedDateTime fiveDaysAgo = getTimeSubtractingDays(5);
    private final ZonedDateTime threeDaysAgo = getTimeSubtractingDays(3);
    private final ZonedDateTime oneDaysAgo = getTimeSubtractingDays(1);


    @Test
    public void testInclusiveEndIsSmallerThanExclusiveEnd() {
        // Arrange
        ZonedInterval interval = new ZonedInterval(sevenDaysAgo, fiveDaysAgo);

        // Assert
        assertTrue("Inclusive time should be smaller", interval.getEndInclusive().isBefore(interval.getEndExclusive()));
    }

    @Test
    public void testDisjointInterval() {
        // Arrange
        ZonedInterval earliestInterval = new ZonedInterval(sevenDaysAgo, fiveDaysAgo);
        ZonedInterval latestInterval = new ZonedInterval(threeDaysAgo, oneDaysAgo);

        // Act
        ZonedInterval obtainedInterval = earliestInterval.span(latestInterval);

        // Assert
        assertEquals("Wrong start time", sevenDaysAgo, obtainedInterval.getStart());
        assertEquals("Wrong end time", oneDaysAgo, obtainedInterval.getEndExclusive());
    }

    @Test
    public void testOverlappingInterval() {
        // Arrange
        ZonedInterval earliestInterval = new ZonedInterval(sevenDaysAgo, threeDaysAgo);
        ZonedInterval latestInterval = new ZonedInterval(fiveDaysAgo, oneDaysAgo);

        // Act
        ZonedInterval obtainedInterval = earliestInterval.span(latestInterval);

        // Assert
        assertEquals("Wrong start time", sevenDaysAgo, obtainedInterval.getStart());
        assertEquals("Wrong end time", oneDaysAgo, obtainedInterval.getEndExclusive());
    }

    @Test
    public void testSubsetInterval() {
        // Arrange
        ZonedInterval earliestInterval = new ZonedInterval(sevenDaysAgo, oneDaysAgo);
        ZonedInterval latestInterval = new ZonedInterval(threeDaysAgo, fiveDaysAgo);

        // Act
        ZonedInterval obtainedInterval = earliestInterval.span(latestInterval);

        // Assert
        assertEquals("Wrong start time", sevenDaysAgo, obtainedInterval.getStart());
        assertEquals("Wrong end time", oneDaysAgo, obtainedInterval.getEndExclusive());
    }

    @Test
    public void testNullInterval() {
        // Arrange
        ZonedInterval interval = new ZonedInterval(sevenDaysAgo, oneDaysAgo);

        // Act
        ZonedInterval obtainedInterval = interval.span(null);

        // Assert
        assertEquals("Wrong start time", sevenDaysAgo, obtainedInterval.getStart());
        assertEquals("Wrong end time", oneDaysAgo, obtainedInterval.getEndExclusive());
    }

    private ZonedDateTime getTimeSubtractingDays(int days) {
        ZonedDateTime nowAtUTC = ZonedDateTime.now(ZoneOffset.UTC);
        return nowAtUTC.minusDays(days).truncatedTo(ChronoUnit.HOURS);
    }
}
