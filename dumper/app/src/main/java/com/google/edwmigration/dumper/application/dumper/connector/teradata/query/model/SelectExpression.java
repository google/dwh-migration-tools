/*
 * Copyright 2022-2025 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.stream;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.ExpressionSerializer;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder;
import java.util.Optional;
import java.util.OptionalLong;

@AutoValue
public abstract class SelectExpression implements Expression {

  public abstract OptionalLong topRowCount();

  public abstract ImmutableList<Projection> projections();

  public abstract Optional<SourceSpec> sourceSpec();

  public abstract Optional<Expression> whereCondition();

  public abstract ImmutableList<OrderBySpec> orderBySpecs();

  public static Builder builder() {
    return new AutoValue_SelectExpression.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTopRowCount(long value);

    abstract Builder setProjections(ImmutableList<Projection> projections);

    abstract Builder setSourceSpec(SourceSpec sourceSpec);

    abstract Optional<SourceSpec> sourceSpec();

    abstract Builder setWhereCondition(Expression whereCondition);

    abstract Builder setOrderBySpecs(ImmutableList<OrderBySpec> orderBySpecs);

    abstract ImmutableList.Builder<OrderBySpec> orderBySpecsBuilder();

    abstract SelectExpression autoBuild();

    SelectExpression build() {
      SelectExpression selectExpression = autoBuild();
      selectExpression
          .topRowCount()
          .ifPresent(
              topRowCount ->
                  Preconditions.checkState(
                      topRowCount > 0, "SELECT TOP must use positive integer."));
      Preconditions.checkState(
          !selectExpression.projections().isEmpty(), "SELECT requires at least one projection.");
      return selectExpression;
    }
  }

  public static SelectBuilder select(String... columns) {
    return new SelectBuilder(
        stream(columns)
            .map(Identifier::create)
            .map(TeradataSelectBuilder::projection)
            .collect(toImmutableList()));
  }

  public static UnionExpression union(SelectExpression... selectExpressions) {
    return UnionExpression.create(ImmutableList.copyOf(selectExpressions));
  }

  public static UnionExpression union(ImmutableList<SelectExpression> selectExpressions) {
    return UnionExpression.create(selectExpressions);
  }

  public static SelectBuilder select(Projection... projections) {
    return new SelectBuilder(ImmutableList.copyOf(projections));
  }

  public static SelectBuilder select(ImmutableList<Projection> projections) {
    return new SelectBuilder(projections);
  }

  public static SelectBuilder selectTop(long rowCount, Projection... projections) {
    return new SelectBuilder(rowCount, ImmutableList.copyOf(projections));
  }

  public abstract static class SelectExpressionBuilder {
    protected Builder builder;

    private SelectExpressionBuilder(Builder builder) {
      this.builder = builder;
    }

    public final SelectExpression build() {
      return builder.build();
    }

    public final String serialize() {
      return ExpressionSerializer.serialize(build());
    }
  }

  public static class SelectBuilder extends SelectExpressionBuilder {

    private SelectBuilder(ImmutableList<Projection> projections) {
      super(builder().setProjections(projections));
    }

    private SelectBuilder(long rowCount, ImmutableList<Projection> projections) {
      super(builder().setTopRowCount(rowCount).setProjections(projections));
    }

    public FromClauseStepBuilder from(String tableName) {
      return new FromClauseStepBuilder(
          builder.setSourceSpec(
              TableSourceSpec.create(Identifier.create(tableName), /* alias= */ Optional.empty())));
    }

    public FromClauseStepBuilder from(SubqueryExpression subquery) {
      return new FromClauseStepBuilder(
          builder.setSourceSpec(
              SubquerySourceSpec.create(subquery, /* alias= */ Optional.empty())));
    }
  }

  public static class FromClauseStepBuilder extends FromClauseAsStepBuilder {

    private FromClauseStepBuilder(Builder builder) {
      super(builder);
    }

    public FromClauseAsStepBuilder as(String alias) {
      return new FromClauseAsStepBuilder(
          builder.setSourceSpec(
              builder
                  .sourceSpec()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "Internal error: Attempt to set alias for non-existent source spec."))
                  .as(alias)));
    }
  }

  public static class FromClauseAsStepBuilder extends AfterWhereStepBuilder {
    private FromClauseAsStepBuilder(Builder builder) {
      super(builder);
    }

    public AfterWhereStepBuilder where(Expression condition) {
      return new AfterWhereStepBuilder(builder.setWhereCondition(condition));
    }
  }

  public static class AfterWhereStepBuilder extends SelectExpressionBuilder {

    private AfterWhereStepBuilder(Builder builder) {
      super(builder);
    }

    public SelectExpression orderBy(OrderBySpec... orderBySpecs) {
      return builder.setOrderBySpecs(ImmutableList.copyOf(orderBySpecs)).build();
    }
  }
}
