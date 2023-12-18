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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.formatQuery;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.OrderBySpec.Direction.DESC;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.BinaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.CastExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Identifier;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.InExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.IntegerLiteral;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.NaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.OrderBySpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Projection;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectSubqueryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SourceSpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.StringLiteral;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubqueryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubquerySourceSpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SubstrExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.TableSourceSpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.UnionExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.UnionSubqueryExpression;
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
    return formatQuery(serializedQuery.toString());
  }

  private void append(Expression expr) {
    serializedQuery.append(' ');
    if (expr instanceof SelectExpression) {
      append((SelectExpression) expression);
    } else if (expr instanceof StringLiteral) {
      serializedQuery.append(escapeStringLiteral(((StringLiteral) expr).value()));
    } else if (expr instanceof IntegerLiteral) {
      serializedQuery.append(((IntegerLiteral) expr).value());
    } else if (expr instanceof Identifier) {
      serializedQuery.append(((Identifier) expr).name());
    } else if (expr instanceof BinaryExpression) {
      append(((BinaryExpression) expr));
    } else if (expr instanceof UnionExpression) {
      append(((UnionExpression) expr));
    } else if (expr instanceof SubqueryExpression) {
      append(((SubqueryExpression) expr));
    } else if (expr instanceof NaryExpression) {
      append(((NaryExpression) expr));
    } else if (expr instanceof InExpression) {
      append(((InExpression) expr));
    } else if (expr instanceof SubstrExpression) {
      append(((SubstrExpression) expr));
    } else if (expr instanceof CastExpression) {
      append(((CastExpression) expr));
    } else {
      throw new IllegalArgumentException(String.format("Unsupported expression type: '%s'.", expr));
    }
  }

  private void append(CastExpression expr) {
    serializedQuery.append("CAST(");
    append(expr.underlyingExpression());
    serializedQuery.append(" AS ");
    append(expr.destinationType());
    serializedQuery.append(')');
  }

  private void append(SubstrExpression expr) {
    serializedQuery.append("SUBSTR(");
    append(expr.string());
    serializedQuery.append(", ");
    append(expr.from());
    serializedQuery.append(", ");
    append(expr.length());
    serializedQuery.append(')');
  }

  private void append(InExpression expr) {
    append(expr.lhs());
    serializedQuery.append(" IN (");
    appendCommaSeparated(expr.items(), this::append);
    serializedQuery.append(')');
  }

  private void append(NaryExpression expr) {
    serializedQuery.append('(');
    appendWithSeparators(expr.subexpressions(), expr.operator().serializedForm, this::append);
    serializedQuery.append(')');
  }

  private void append(BinaryExpression expr) {
    append(expr.lhs());
    serializedQuery.append(' ');
    serializedQuery.append(expr.operator());
    serializedQuery.append(' ');
    append(expr.rhs());
  }

  private void append(SubqueryExpression expr) {
    serializedQuery.append('(');
    if (expr instanceof SelectSubqueryExpression) {
      append(((SelectSubqueryExpression) expr).selectExpression());
    } else if (expr instanceof UnionSubqueryExpression) {
      append(((UnionSubqueryExpression) expr).unionExpression());
    } else {
      throw new IllegalStateException(String.format("Unsupported subquery expression '%s'", expr));
    }
    serializedQuery.append(')');
  }

  private void append(UnionExpression expr) {
    appendWithSeparators(expr.selectExpressions(), "UNION ALL", this::append);
  }

  private void append(SelectExpression selectExpression) {
    serializedQuery.append("SELECT");
    selectExpression
        .topRowCount()
        .ifPresent(rowCount -> serializedQuery.append(" TOP ").append(rowCount));
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
    if (!selectExpression.orderBySpecs().isEmpty()) {
      serializedQuery.append(" ORDER BY");
      appendCommaSeparated(selectExpression.orderBySpecs(), this::append);
    }
  }

  private void append(OrderBySpec orderBySpec) {
    append(orderBySpec.expression());
    if (orderBySpec.direction() == DESC) {
      serializedQuery.append(" DESC");
    }
  }

  private void append(SourceSpec sourceSpec) {
    serializedQuery.append(' ');
    if (sourceSpec instanceof TableSourceSpec) {
      append((TableSourceSpec) sourceSpec);
    } else if (sourceSpec instanceof SubquerySourceSpec) {
      append((SubquerySourceSpec) sourceSpec);
    } else {
      throw new IllegalStateException(String.format("Unsupported source spec='%s'.", sourceSpec));
    }
  }

  private void append(SubquerySourceSpec sourceSpec) {
    append(sourceSpec.subqueryExpression());
    append(sourceSpec.alias());
  }

  private void append(TableSourceSpec tableSourceSpec) {
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

  private <T> void appendCommaSeparated(ImmutableList<T> list, Consumer<T> appender) {
    appendWithSeparators(list, ",", appender);
  }

  private <T> void appendWithSeparators(
      ImmutableList<T> list, String separator, Consumer<T> appender) {
    boolean first = true;
    for (T element : list) {
      if (first) {
        first = false;
      } else {
        if (!separator.equals(",")) {
          serializedQuery.append(' ');
        }
        serializedQuery.append(separator);
      }
      serializedQuery.append(' ');
      appender.accept(element);
    }
  }

  private static String escapeStringLiteral(String s) {
    return "'" + (s.replaceAll("'", "''")) + "'";
  }
}
