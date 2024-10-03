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

import com.google.edwmigration.dumper.application.dumper.QueryLogSharedState.QueryLogEntry;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryLogSharedStateTest {

  @Before
  public void beforeEachTest() {
    QueryLogSharedState.clearQueryLogEntries();
  }

  @Test
  public void queryLogFirstEntryUpdatedSuccessfully() {
    // Arrange
    ZonedDateTime newQueryLogDate = ZonedDateTime.now();

    // Act
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_FIRST_ENTRY, newQueryLogDate);

    // Assert
    assertEquals(
        newQueryLogDate,
        QueryLogSharedState.queryLogEntries.get(QueryLogEntry.QUERY_LOG_FIRST_ENTRY));
  }

  @Test
  public void queryLogLastEntryUpdatedSuccessfully() {
    // Arrange
    ZonedDateTime newQueryLogDate = ZonedDateTime.now();

    // Act
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_LAST_ENTRY, newQueryLogDate);

    // Assert
    assertEquals(
        newQueryLogDate,
        QueryLogSharedState.queryLogEntries.get(QueryLogEntry.QUERY_LOG_LAST_ENTRY));
  }

  @Test
  public void queryLogFirstEntryUpdatedSuccessfullyForEarlierDate() {
    // Arrange
    ZonedDateTime now = ZonedDateTime.now();
    ZonedDateTime olderDate = ZonedDateTime.of(1970, 1, 1, 1, 1, 1, 1, ZoneId.of("UTC"));

    // Act
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_FIRST_ENTRY, now);
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_FIRST_ENTRY, olderDate);

    // Assert
    assertEquals(
        olderDate, QueryLogSharedState.queryLogEntries.get(QueryLogEntry.QUERY_LOG_FIRST_ENTRY));
  }

  @Test
  public void queryLogFirstEntryUpdatedSuccessfullyForLaterDate() {
    // Arrange
    ZonedDateTime date = ZonedDateTime.of(2000, 1, 1, 1, 1, 1, 1, ZoneId.of("UTC"));
    ZonedDateTime laterDate = ZonedDateTime.now();

    // Act
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_LAST_ENTRY, date);
    QueryLogSharedState.updateQueryLogEntries(QueryLogEntry.QUERY_LOG_LAST_ENTRY, laterDate);

    // Assert
    assertEquals(
        laterDate, QueryLogSharedState.queryLogEntries.get(QueryLogEntry.QUERY_LOG_LAST_ENTRY));
  }
}
