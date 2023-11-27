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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.add;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.cast;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.integerLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.multiply;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.projection;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.substr;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.subtract;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.union;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.ExpressionSerializer;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Projection;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.FromClauseStepBuilder;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Generator for queries that select from the table that contains a column of type VARCHAR, allowing
 * it to be split into multiple rows in order to avoid exceeding the row size limit.
 *
 * <p>In case the VARCHAR column is not split and the 64kB row size limit is exceeded, Teradata
 * server returns the error: <code>
 * java.sql.SQLException: [Teradata Database] [TeraJDBC 16.20.00.10] [Error 9804] [SQLState HY000]
 * Response Row size or Constant Row size overflow.
 * </code>
 */
public class SplitTextColumnQueryGenerator {

  private static final int MAX_PARTS_AFTER_SPLIT = 10;

  private final ImmutableList<String> columnNames;
  private final String textColumnName;
  private final String counterColumnName;
  private final String tableName;
  private final Optional<Expression> whereCondition;
  private final int textColumnOriginalLength;
  private final int splitTextColumnMaxLength;

  public SplitTextColumnQueryGenerator(
      ImmutableList<String> columnNames,
      String textColumnName,
      String counterColumnName,
      String tableName,
      Optional<Expression> whereCondition,
      int textColumnOriginalLength,
      int splitTextColumnMaxLength) {
    Preconditions.checkArgument(
        textColumnOriginalLength > 0, "textColumnOriginalLength must be greater than 0");
    Preconditions.checkArgument(
        splitTextColumnMaxLength > 0, "splitTextColumnMaxLength must be greater than 0");
    int partsCount =
        IntMath.divide(textColumnOriginalLength, splitTextColumnMaxLength, RoundingMode.CEILING);
    Preconditions.checkArgument(
        partsCount <= MAX_PARTS_AFTER_SPLIT,
        "Too many parts after splitting. Original length='%s', splitTextColumnMaxLength='%s',"
            + " parts count='%s', max parts count='%s'.",
        textColumnOriginalLength,
        splitTextColumnMaxLength,
        partsCount,
        MAX_PARTS_AFTER_SPLIT);
    this.columnNames = columnNames;
    this.textColumnName = textColumnName;
    this.counterColumnName = counterColumnName;
    this.tableName = tableName;
    this.whereCondition = whereCondition;
    this.textColumnOriginalLength = textColumnOriginalLength;
    this.splitTextColumnMaxLength = splitTextColumnMaxLength;
  }

  public String generate() {
    int partsCount =
        IntMath.divide(textColumnOriginalLength, splitTextColumnMaxLength, RoundingMode.CEILING);
    return ExpressionSerializer.serialize(
        union(
            IntStream.range(0, partsCount)
                .mapToObj(i -> createSubquery(i, splitTextColumnMaxLength, partsCount))
                .collect(toImmutableList())));
  }

  private SelectExpression createSubquery(int partIndex, int length, int partsCount) {
    ImmutableList.Builder<Projection> projections = ImmutableList.builder();
    columnNames.stream()
        .map(columnName -> projection(identifier(columnName)))
        .forEach(projections::add);
    if (partsCount > 1) {
      projections.add(
          cast(
                  substr(
                      identifier(textColumnName),
                      integerLiteral(1 + partIndex * length),
                      integerLiteral(length)),
                  identifier("VARCHAR(" + length + ")"))
              .as(textColumnName));
      projections.add(
          add(
                  multiply(
                      subtract(identifier(counterColumnName), integerLiteral(1)),
                      integerLiteral(partsCount)),
                  integerLiteral(partIndex + 1))
              .as(counterColumnName));
    } else {
      projections.add(projection(identifier(textColumnName)));
      projections.add(projection(identifier(counterColumnName)));
    }
    FromClauseStepBuilder builder = select(projections.build()).from(tableName);
    return whereCondition.map(builder::where).orElse(builder).build();
  }
}
