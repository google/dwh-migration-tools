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

import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.assertQueryEquals;
import static java.util.Collections.emptyList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.AbstractTeradataConnector.SharedState;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.OptionalLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TeradataAssessmentLogsJdbcTaskTest {

  private SharedState queryLogsState = new SharedState();
  private ZonedInterval interval =
      new ZonedInterval(
          ZonedDateTime.of(2023, 3, 4, 16, 0, 0, 0, ZoneOffset.UTC),
          ZonedDateTime.of(2023, 3, 4, 17, 0, 0, 0, ZoneOffset.UTC));

  @Before
  public void setUp() {
    System.setProperty("user.timezone", "UTC");
  }

  @Test
  public void getOrCreateSql_success() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.empty(),
            /* orderBy */ emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getOrCreateSql_predicateFilteringByPrefix() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.empty(),
            /* orderBy */ emptyList());

    // Act
    String query =
        jdbcTask.getOrCreateSql(
            s -> s.startsWith("ST"), ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT NULL, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN SampleSqlTable ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getOrCreateSql_maxSqlLength() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.of(20000),
            /* orderBy */ emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN ("
            + "   SELECT QueryID,"
            + "     CAST(SUBSTR(SqlTextInfo, 1, 20000) AS VARCHAR(20000)) AS SqlTextInfo,"
            + "     (((SqlRowNo - 1) * 2) + 1) AS SqlRowNo FROM SampleSqlTable"
            + "   UNION ALL "
            + "   SELECT QueryID,"
            + "     CAST(SUBSTR(SqlTextInfo, 20001, 20000) AS VARCHAR(20000)) AS SqlTextInfo,"
            + "     (((SqlRowNo - 1) * 2) + 2) AS SqlRowNo FROM SampleSqlTable"
            + " ) ST ON (L.QueryID=ST.QueryID)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getOrCreateSql_noSecondTable() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.empty(),
            /* orderBy= */ emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)",
        query);
  }

  @Test
  public void getOrCreateSql_noSecondTableWithLogDateColumn() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            "SampleLogDate", /* orderBy */
            /* maxSqlLength= */ OptionalLong.empty(),
            emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND"
            + " L.SampleLogDate = CAST('2023-03-04Z' AS DATE)",
        query);
  }

  @Test
  public void getOrCreateSql_noSecondTableWithCondition() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of("L.QueryID=7"),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.empty(),
            /* orderBy= */ emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND L.QueryID=7",
        query);
  }

  @Test
  public void getOrCreateSql_noSecondTableWithOrderBy() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            /* logDateColumn= */ null,
            /* maxSqlLength= */ OptionalLong.empty(),
            /* orderBy= */ ImmutableList.of("L.QueryID", "L.QueryText"));

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID"
            + " FROM SampleQueryTable L"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP)"
            + " ORDER BY L.QueryID, L.QueryText",
        query);
  }

  @Test
  public void getOrCreateSql_withLogDateColumn() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            "SampleLogDate", /* orderBy */
            /* maxSqlLength= */ OptionalLong.empty(),
            emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
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
  public void getOrCreateSql_withLogDateColumnAndMaxSqlLength() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            /* conditions= */ ImmutableSet.of(),
            interval,
            "SampleLogDate", /* orderBy */
            /* maxSqlLength= */ OptionalLong.of(20000),
            emptyList());

    // Act
    String query = jdbcTask.getOrCreateSql(s -> true, ImmutableList.of("L.QueryID", "ST.QueryID"));

    // Assert
    assertQueryEquals(
        "SELECT L.QueryID, ST.QueryID"
            + " FROM SampleQueryTable L LEFT OUTER JOIN ("
            + "   SELECT QueryID, SampleLogDate,"
            + "     CAST(SUBSTR(SqlTextInfo, 1, 20000) AS VARCHAR(20000)) AS SqlTextInfo,"
            + "     (((SqlRowNo - 1) * 2) + 1) AS SqlRowNo FROM SampleSqlTable"
            + "     WHERE SampleLogDate = CAST('2023-03-04Z' AS DATE)"
            + "   UNION ALL "
            + "   SELECT QueryID, SampleLogDate,"
            + "     CAST(SUBSTR(SqlTextInfo, 20001, 20000) AS VARCHAR(20000)) AS SqlTextInfo,"
            + "     (((SqlRowNo - 1) * 2) + 2) AS SqlRowNo FROM SampleSqlTable"
            + "     WHERE SampleLogDate = CAST('2023-03-04Z' AS DATE)"
            + " ) ST"
            + " ON (L.QueryID=ST.QueryID AND L.SampleLogDate=ST.SampleLogDate)"
            + " WHERE L.ErrorCode=0 AND"
            + " L.StartTime >= CAST('2023-03-04T16:00:00Z' AS TIMESTAMP) AND"
            + " L.StartTime < CAST('2023-03-04T17:00:00Z' AS TIMESTAMP) AND"
            + " L.SampleLogDate = CAST('2023-03-04Z' AS DATE)",
        query);
  }

  @Test
  public void getOrCreateSql_fullQuery() {
    TeradataAssessmentLogsJdbcTask jdbcTask =
        new TeradataAssessmentLogsJdbcTask(
            "result.csv",
            queryLogsState,
            createTableName("SampleQueryTable", "SampleSqlTable"),
            ImmutableSet.of("QueryID=7", "QueryText LIKE '%abc%'"),
            interval,
            "SampleLogDate",
            /* maxSqlLength= */ OptionalLong.empty(),
            ImmutableList.of("ST.QueryID", "ST.RowNo"));

    // Act
    String query =
        jdbcTask.getOrCreateSql(
            s -> true, ImmutableList.of("L.QueryID", "L.QueryText", "ST.QueryID"));

    // Assert
    assertQueryEquals(
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

  private QueryLogTableNames createTableName(String logTable, String sqlTable) {
    return QueryLogTableNames.create(logTable, sqlTable, /* usingAtLeastOneAlternate */ true);
  }
}
