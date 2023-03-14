package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class TeradataSqlQueryFactoryTest {

  @Test
  public void testGetSql() {
    //  Arrange
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    TeradataSqlQueryFactory factory =
        new TeradataSqlQueryFactory(
            interval,
            "LT",
            "QT",
            "",
            "LT L",
            "TF",
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList("x", "y", "z"),
            false);
    String expectedQuery =
        "SELECT "
            + "x, y, z "
            + "FROM LT L "
            + "WHERE TF >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) "
            + "AND TF < CAST('2023-03-09T00:00:00Z' AS TIMESTAMP)";

    // Act & Assert
    checkGeneratedQuery(factory, expectedQuery);
  }

  @Test
  public void testGetSql_forAddedConditions() {
    //  Arrange
    ZonedDateTime endDate = ZonedDateTime.parse("2023-03-09T00:00:00Z");
    ZonedDateTime startDate = endDate.minusDays(5).truncatedTo(ChronoUnit.DAYS);
    ZonedInterval interval = new ZonedInterval(startDate, endDate);
    TeradataSqlQueryFactory factory =
        new TeradataSqlQueryFactory(
            interval,
            "LT",
            "QT",
            "",
            "LT L",
            "TF",
            Arrays.asList("y > 10", "z <> 'NOT AVAILABLE'"),
            Collections.emptyList(),
            Collections.singletonList("x"),
            false);
    String expectedQuery =
        "SELECT "
            + "x "
            + "FROM LT L "
            + "WHERE TF >= CAST('2023-03-04T00:00:00Z' AS TIMESTAMP) "
            + "AND TF < CAST('2023-03-09T00:00:00Z' AS TIMESTAMP) "
            + "AND y > 10 "
            + "AND z <> 'NOT AVAILABLE'";

    // Act & Assert
    checkGeneratedQuery(factory, expectedQuery);
  }

  private void checkGeneratedQuery(TeradataSqlQueryFactory factory, String expectedQuery) {
    assertEquals(expectedQuery, factory.getSql((c, t) -> true));
  }
}
