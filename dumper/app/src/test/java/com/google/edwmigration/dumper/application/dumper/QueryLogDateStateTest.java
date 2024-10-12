/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryLogDateStateTest {

  @Test
  public void updateQueryLogFirstEntry_shouldUpdateToNewValue() {
    // Arrange
    ZonedDateTime queryLogStartDate = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

    // Act
    QueryLogDateState.updateQueryLogFirstEntry(queryLogStartDate);

    // Assert
    assertEquals(QueryLogDateState.getQueryLogFirstEntry(), queryLogStartDate);
  }

  @Test
  public void updateQueryLogFirstEntry_shouldNotUpdateToNewValue() {
    // Arrange
    ZonedDateTime queryLogStartDate = ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

    // Act
    QueryLogDateState.updateQueryLogFirstEntry(queryLogStartDate);

    // Assert
    assertNotEquals(QueryLogDateState.getQueryLogFirstEntry(), queryLogStartDate);
  }

  @Test
  public void updateQueryLogLastEntry_shouldUpdateToNewValue() {
    // Arrange
    ZonedDateTime queryLogStartDate = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

    // Act
    QueryLogDateState.updateQueryLogLastEntry(queryLogStartDate);

    // Assert
    assertEquals(QueryLogDateState.getQueryLogFirstEntry(), queryLogStartDate);
  }

  @Test
  public void updateQueryLogLastEntry_shouldNotUpdateToNewValue() {
    // Arrange
    ZonedDateTime queryLogStartDate = ZonedDateTime.of(1960, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

    // Act
    QueryLogDateState.updateQueryLogLastEntry(queryLogStartDate);

    // Assert
    assertNotEquals(QueryLogDateState.getQueryLogFirstEntry(), queryLogStartDate);
  }
}
