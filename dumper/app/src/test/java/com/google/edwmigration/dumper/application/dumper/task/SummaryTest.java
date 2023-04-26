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

import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.getTimeSubtractingDays;

import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.time.ZonedDateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SummaryTest {

  private final ZonedDateTime SEVEN_DAYS_AGO = getTimeSubtractingDays(7);
  private final ZonedDateTime FIVE_DAYS_AGO = getTimeSubtractingDays(5);
  private final ZonedDateTime THREE_DAYS_AGO = getTimeSubtractingDays(3);
  private final ZonedDateTime ONE_DAY_AGO = getTimeSubtractingDays(1);

  @Test
  public void testCombine_combinesTwoSummaries() {
    // Arrange
    Summary s1 = new Summary(5).withInterval(new ZonedInterval(SEVEN_DAYS_AGO, FIVE_DAYS_AGO));
    Summary s2 = new Summary(12).withInterval(new ZonedInterval(THREE_DAYS_AGO, ONE_DAY_AGO));
    Summary expectedSummary =
        new Summary(17).withInterval(new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO));

    // Act
    Summary resultingSummary = Summary.COMBINER.apply(s1, s2);

    //  Assert
    Assert.assertEquals(expectedSummary, resultingSummary);
  }

  @Test
  public void testCombine_combinesEmptyAndNonEmptySummary() {
    // Arrange
    ZonedInterval expectedInterval = new ZonedInterval(SEVEN_DAYS_AGO, ONE_DAY_AGO);
    Summary s1 = new Summary(10);
    Summary s2 = new Summary(12).withInterval(expectedInterval);
    Summary expectedSummary = new Summary(22).withInterval(expectedInterval);

    // Act
    Summary resultingSummary = Summary.COMBINER.apply(s1, s2);

    //  Assert
    Assert.assertEquals(expectedSummary, resultingSummary);
  }

  @Test
  public void testCombine_combinesTwoEmptySummaries() {
    // Arrange
    Summary s1 = new Summary(10);
    Summary s2 = new Summary(12);
    Summary expectedSummary = new Summary(22);

    // Act
    Summary resultingSummary = Summary.COMBINER.apply(s1, s2);

    //  Assert
    Assert.assertEquals(expectedSummary, resultingSummary);
  }
}
