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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TeradataAssessmentLogsJdbcTaskTest {

  private SharedState queryLogsState = new SharedState();
  private ZonedInterval interval =
      new ZonedInterval(
          ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneId.systemDefault()),
          ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneId.systemDefault()));

  @Test
  public void getSql_success() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ emptyList(),
            interval,
            /* logDateColumn= */ null,
            /* orderBy */ emptyList());

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID", "ST.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getSql_noSecondTable() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ emptyList(),
            interval,
            /* logDateColumn= */ null,
            /* orderBy= */ emptyList());

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getSql_noSecondTableWithLogDateColumn() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ emptyList(),
            interval,
            "SampleLogDate", /* orderBy */
            emptyList());

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND"
            + " L.SampleLogDate = CAST('2023-03-04Z' AS DATE)",
        query);
  }

  @Test
  public void getSql_noSecondTableWithCondition() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ ImmutableList.of("L.QueryID=7"),
            interval,
            /* logDateColumn= */ null,
            /* orderBy= */ emptyList());

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND L.QueryID=7",
        query);
  }

  @Test
  public void getSql_noSecondTableWithOrderBy() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ emptyList(),
            interval,
            /* logDateColumn= */ null,
            /* orderBy= */ ImmutableList.of("L.QueryID", "L.QueryText"));

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)"
            + " ORDER BY L.QueryID, L.QueryText",
        query);
  }

  @Test
  public void getSql_withLogDateColumn() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            /* conditions= */ emptyList(),
            interval,
            "SampleLogDate", /* orderBy */
            emptyList());

    String query = jdbcTask.getSql(s -> true, new String[] {"L.QueryID", "ST.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST"
            + " ON (L.QueryID=ST.QueryID AND L.SampleLogDate=ST.SampleLogDate)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND"
            + " L.SampleLogDate = CAST('2023-03-04Z' AS DATE)",
        query);
  }

  @Test
  public void getSql_fullQuery() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            "SampleQueryTable",
            "SampleSqlTable",
            ImmutableList.of("QueryID=7", "QueryText LIKE '%abc%'"),
            interval,
            "SampleLogDate",
            ImmutableList.of("ST.QueryID", "ST.RowNo"));

    String query =
        jdbcTask.getSql(s -> true, new String[] {"L.QueryID", "L.QueryText", "ST.QueryID"});

    assertEqualsIgnoringWhitespace(
        "SELECT L.QueryID, L.QueryText, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST"
            + " ON (L.QueryID=ST.QueryID AND L.SampleLogDate=ST.SampleLogDate)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND"
            + " L.SampleLogDate = CAST('2023-03-04Z' AS DATE) AND"
            + " QueryID=7 AND QueryText LIKE '%abc%'"
            + " ORDER BY ST.QueryID, ST.RowNo",
        query);
  }

  private static void assertEqualsIgnoringWhitespace(String expected, String actual) {
    assertEquals(squashWhitespace(expected), squashWhitespace(actual));
  }

  private static String squashWhitespace(String s) {
    return s.replaceFirst("^\\s+", "").replaceFirst("\\s+$", "").replaceAll("\\s+", " ");
  }

  private static ZonedDateTime createUTCZonedDateTime(long epochSecond) {
    return ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneId.systemDefault());
  }
}
