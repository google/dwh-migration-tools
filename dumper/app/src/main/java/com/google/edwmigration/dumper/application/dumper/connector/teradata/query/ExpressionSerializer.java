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
package com.google.edwmigration.dumper.application.dumper.connector.teradata.query;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.BinaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Identifier;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Projection;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SourceSpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.StringLiteral;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.TableSourceSpec;
import java.util.Optional;
import java.util.function.Consumer;

public class ExpressionSerializer {

  private final Expression expression;
  private StringBuilder serializedQuery = new StringBuilder();

  ExpressionSerializer(Expression expression) {
    this.expression = expression;
  }

  public static String serialize(Expression expression) {
    return new ExpressionSerializer(expression).serialize();
  }

  String serialize() {
    serializedQuery = new StringBuilder();
    append(expression);
    return serializedQuery.toString();
  }

  private void append(Expression expr) {
    if (expr instanceof SelectExpression) {
      append((SelectExpression) expression);
    } else if (expr instanceof StringLiteral) {
      serializedQuery.append(escapeStringLiteral(((StringLiteral) expr).value()));
    } else if (expr instanceof Identifier) {
      serializedQuery.append(((Identifier) expr).name());
    } else if (expr instanceof BinaryExpression) {
      append(((BinaryExpression) expr));
    } else {
      throw new IllegalArgumentException(String.format("Unsupported expression type: '%s'.", expr));
    }
  }

  private void append(BinaryExpression expr) {
    appendSpaceIfNecessary();
    append(expr.lhs());
    serializedQuery.append(' ');
    serializedQuery.append(expr.operator());
    serializedQuery.append(' ');
    append(expr.rhs());
  }

  private void append(SelectExpression selectExpression) {
    serializedQuery.append("SELECT");
    appendCommaSeparated(selectExpression.projections(), this::append);
    selectExpression
        .sourceSpec()
        .ifPresent(
            sourceSpec -> {
              serializedQuery.append(" FROM");
              append(sourceSpec);
            });
    selectExpression
        .whereCondition()
        .ifPresent(
            condition -> {
              serializedQuery.append(" WHERE");
              append(condition);
            });
  }

  private void append(SourceSpec sourceSpec) {
    if (sourceSpec instanceof TableSourceSpec) {
      append((TableSourceSpec) sourceSpec);
    } else {
      throw new IllegalStateException(String.format("Unsupported source spec='%s'.", sourceSpec));
    }
  }

  private void append(TableSourceSpec tableSourceSpec) {
    serializedQuery.append(' ');
    append(tableSourceSpec.tableName());
    append(tableSourceSpec.alias());
  }

  private void append(Projection projection) {
    append(projection.expression());
    append(projection.alias());
  }

  private void append(Optional<String> aliasMaybe) {
    aliasMaybe.ifPresent(alias -> serializedQuery.append(" AS ").append(alias));
  }

  private void appendSpaceIfNecessary() {
    if (serializedQuery.length() > 0) {
      serializedQuery.append(' ');
    }
  }

  private <T> void appendCommaSeparated(ImmutableList<T> list, Consumer<T> appender) {
    boolean first = true;
    for (T element : list) {
      if (first) {
        first = false;
      } else {
        serializedQuery.append(',');
      }
      serializedQuery.append(' ');
      appender.accept(element);
    }
  }

  private static String escapeStringLiteral(String s) {
    return "'" + (s.replaceAll("'", "''")) + "'";
  }
}
