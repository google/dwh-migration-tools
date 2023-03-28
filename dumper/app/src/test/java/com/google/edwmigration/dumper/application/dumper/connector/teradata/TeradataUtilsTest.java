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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TeradataUtilsTest {

  @Test
  public void createTimestampExpression_success() {
    String timestampExpression = TeradataUtils.createTimestampExpression("abc");

    assertEquals("abc AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"abc\"", timestampExpression);
  }

  @Test
  public void createTimestampExpression_empty_throwsException() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> TeradataUtils.createTimestampExpression(""));

    assertEquals("Column name must not be empty.", e.getMessage());
  }

  @Test
  public void createTimestampExpression_withAlias_success() {
    String timestampExpression = TeradataUtils.createTimestampExpression("t", "abc");

    assertEquals(
        "t.abc AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"abc\"", timestampExpression);
  }

  @Test
  public void createTimestampExpression_emptyAlias_throwsException() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> TeradataUtils.createTimestampExpression("", "tableName"));

    assertEquals("Alias must not be empty.", e.getMessage());
  }

  @Test
  public void formatQuery_emptyString() {
    String formattedQuery = TeradataUtils.formatQuery("");

    assertEquals("", formattedQuery);
  }

  @Test
  public void formatQuery_space() {
    String formattedQuery = TeradataUtils.formatQuery(" ");

    assertEquals("", formattedQuery);
  }

  @Test
  public void formatQuery_multipleSpacesInQuery() {
    String formattedQuery = TeradataUtils.formatQuery("  SELECT      1     ");

    assertEquals("SELECT 1", formattedQuery);
  }

  @Test
  public void formatQuery_spacesInsideParentheses() {
    String formattedQuery = TeradataUtils.formatQuery("  SELECT  ( 2 + N )     ");

    assertEquals("SELECT (2 + N)", formattedQuery);
  }

  @Test
  public void formatQuery_spacesInsideMultipleParentheses() {
    String formattedQuery =
        TeradataUtils.formatQuery("  SELECT  ( 2 + N + ( 3 + N + ( N +    N )  )  )    ");

    assertEquals("SELECT (2 + N + (3 + N + (N + N)))", formattedQuery);
  }
}
