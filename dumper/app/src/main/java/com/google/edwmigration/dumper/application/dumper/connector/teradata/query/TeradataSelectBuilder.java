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
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.NaryExpression.NaryOperator.AND;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.BinaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.CastExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Identifier;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.InExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.IntegerLiteral;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.NaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.NaryExpression.NaryOperator;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Projection;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.SelectBuilder;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectSubqueryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.StringLiteral;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubqueryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubquerySourceSpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubstrExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.UnionExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.UnionSubqueryExpression;
import java.util.List;
import java.util.Optional;

/** Shortcuts for building the Teradata query. */
public class TeradataSelectBuilder {

  public static Identifier identifier(String name) {
    return Identifier.create(name);
  }

  public static Projection projection(Expression expression) {
    return Projection.create(expression, /* alias= */ Optional.empty());
  }

  public static Projection projection(Expression expression, String alias) {
    return Projection.create(expression, Optional.of(alias));
  }

  public static Expression eq(Expression lhs, Expression rhs) {
    return BinaryExpression.create(lhs, "=", rhs);
  }

  public static Expression in(Expression lhs, List<String> literals) {
    return InExpression.create(
        lhs,
        literals.stream().map(TeradataSelectBuilder::stringLiteral).collect(toImmutableList()));
  }

  public static StringLiteral stringLiteral(String value) {
    return StringLiteral.create(value);
  }

  public static IntegerLiteral integerLiteral(long value) {
    return IntegerLiteral.create(value);
  }

  public static Expression multiply(Expression... expressions) {
    return NaryExpression.create(NaryOperator.MUL, ImmutableList.copyOf(expressions));
  }

  public static Expression add(Expression... expressions) {
    return NaryExpression.create(NaryOperator.ADD, ImmutableList.copyOf(expressions));
  }

  public static Expression subtract(Expression a, Expression b) {
    return NaryExpression.create(NaryOperator.SUBTRACT, ImmutableList.of(a, b));
  }

  public static SubstrExpression substr(Expression string, Expression from, Expression length) {
    return SubstrExpression.create(string, from, length);
  }

  public static CastExpression cast(Expression expression, Expression destinationType) {
    return CastExpression.create(expression, destinationType);
  }

  public static SubquerySourceSpec subquerySource(SelectExpression selectExpression) {
    return SubquerySourceSpec.create(
        SelectSubqueryExpression.create(selectExpression), /* alias= */ Optional.empty());
  }

  public static SubquerySourceSpec subquerySource(UnionExpression unionExpression) {
    return SubquerySourceSpec.create(
        UnionSubqueryExpression.create(unionExpression), /* alias= */ Optional.empty());
  }

  public static SubqueryExpression subquery(UnionExpression unionExpression) {
    return UnionSubqueryExpression.create(unionExpression);
  }

  public static SubqueryExpression subquery(SelectExpression selectExpression) {
    return SelectSubqueryExpression.create(selectExpression);
  }

  public static NaryExpression and(Expression... subexpressions) {
    return NaryExpression.create(AND, ImmutableList.copyOf(subexpressions));
  }

  public static Identifier star() {
    return identifier("*");
  }

  public static SelectBuilder selectAll() {
    return select(projection(star()));
  }
}
