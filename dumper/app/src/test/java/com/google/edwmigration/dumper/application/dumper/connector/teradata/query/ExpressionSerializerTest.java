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
package com.google.edwmigration.dumper.application.dumper.connector.teradata.query;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.add;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.cast;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.eq;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.integerLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.multiply;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.projection;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.stringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.substr;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.subtract;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.selectTop;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.union;
import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.assertQueryEquals;
import static java.util.stream.Stream.concat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.SelectExpressionBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class ExpressionSerializerTest {

  private static final ImmutableList<Pair<Expression, String>> EXPRESSIONS =
      ImmutableList.of(
          Pair.of(identifier("x"), "x"),
          Pair.of(stringLiteral(""), "''"),
          Pair.of(stringLiteral("abc"), "'abc'"),
          Pair.of(stringLiteral("de'f"), "'de''f'"),
          Pair.of(eq(stringLiteral("A"), stringLiteral("BC")), "'A' = 'BC'"),
          Pair.of(
              union(select("col_a").from("tab_1").build(), select("col_b").from("tab_2").build()),
              "SELECT col_a FROM tab_1 UNION ALL SELECT col_b FROM tab_2"),
          Pair.of(integerLiteral(123), "123"),
          Pair.of(
              substr(stringLiteral("abc"), integerLiteral(2), integerLiteral(300)),
              "SUBSTR('abc', 2, 300)"),
          Pair.of(
              cast(stringLiteral("abc"), identifier("VARCHAR(13)")), "CAST('abc' AS VARCHAR(13))"),
          Pair.of(subtract(integerLiteral(7), identifier("x")), "(7 - x)"),
          Pair.of(add(integerLiteral(7), identifier("x"), identifier("y")), "(7 + x + y)"),
          Pair.of(multiply(integerLiteral(7), identifier("x"), identifier("y")), "(7 * x * y)"));

  private static final ImmutableList<Pair<SelectExpressionBuilder, String>> SELECT_QUERIES =
      ImmutableList.of(
          Pair.of(select("x"), "SELECT x"),
          Pair.of(select("1"), "SELECT 1"),
          Pair.of(select(identifier("sampleCol").as("value")), "SELECT sampleCol AS value"),
          Pair.of(select("col1").from("sampleTable"), "SELECT col1 FROM sampleTable"),
          Pair.of(
              select("col1").from("sampleTable").as("st"), "SELECT col1 FROM sampleTable AS st"),
          Pair.of(
              select("col1", "col2", "col3").from("sampleTable"),
              "SELECT col1, col2, col3 FROM sampleTable"),
          Pair.of(
              select("col_a")
                  .from("tab_b")
                  .where(eq(identifier("col_c"), stringLiteral("SUCCESS"))),
              "SELECT col_a FROM tab_b WHERE col_c = 'SUCCESS'"),
          Pair.of(selectTop(10, projection(identifier("x"))), "SELECT TOP 10 x"));

  @DataPoints("expressionsAndQueries")
  public static final ImmutableList<Pair<Expression, String>> EXPRESSIONS_AND_QUERIES =
      concat(
              EXPRESSIONS.stream(),
              SELECT_QUERIES.stream()
                  .map(
                      pair -> Pair.<Expression, String>of(pair.getLeft().build(), pair.getRight())))
          .collect(toImmutableList());

  @Theory
  public void serialize_success(
      @FromDataPoints("expressionsAndQueries") Pair<Expression, String> testCase) {
    Expression expression = testCase.getLeft();
    String expectedSerializedExpression = testCase.getRight();

    // Act
    String serializedExpression = new ExpressionSerializer(expression).serialize();

    // Assert
    assertQueryEquals(expectedSerializedExpression, serializedExpression);
  }

  @Test
  public void serialize_unsupportedExpressionType_fail() {
    class TestExpression implements Expression {}

    // Act
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new ExpressionSerializer(new TestExpression()).serialize());

    // Assert
    assertTrue(e.getMessage().matches("Unsupported expression type: '.+TestExpression.+'."));
  }
}
