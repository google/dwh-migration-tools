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

import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.BinaryExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Identifier;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Projection;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.StringLiteral;
import java.util.Optional;

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

  public static StringLiteral stringLiteral(String value) {
    return StringLiteral.create(value);
  }
}
