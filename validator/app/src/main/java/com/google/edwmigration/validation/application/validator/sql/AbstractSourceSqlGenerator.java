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
package com.google.edwmigration.validation.application.validator.sql;

import static org.jooq.impl.DSL.*;

import autovalue.shaded.com.google.errorprone.annotations.ForOverride;
import java.util.List;
import javax.annotation.Nonnull;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public abstract class AbstractSourceSqlGenerator implements SqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSourceSqlGenerator.class);
  private final DSLContext create;
  private final String table;
  private final Double confidenceInterval;

  public AbstractSourceSqlGenerator(
      @Nonnull SQLDialect dialect, @Nonnull String table, @Nonnull Double confidenceInterval) {
    this.create = DSL.using(dialect);
    this.table = table;
    this.confidenceInterval = confidenceInterval;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  public String getTable() {
    return table;
  }

  public abstract Long getRowCount();

  @ForOverride
  public abstract List<String> getNumericColumns();

  @Override
  public String getAggregateQuery() {
    Table<?> table = DSL.table(getTable());
    SelectSelectStep<?> select = create.select();

    select = select.select(count());

    for (String columnName : getNumericColumns()) {
      Field column = DSL.field(DSL.name(columnName));
      select =
          select
              .select(DSL.sum(column).as("sum_" + columnName))
              .select(DSL.min(column).as("min_" + columnName))
              .select(DSL.max(column).as("max_" + columnName));
    }

    SelectQuery<?> query = select.from(table).getQuery();

    LOG.debug("Aggregate query generated: " + query.getSQL());
    return query.getSQL();
  }

  private static int generateMask(
      Long totalRows, Double confidenceLevel, double marginOfError, double populationProportion) {
    // Calculate the Z-score for the given confidence level
    double zScore = calculateZScore(confidenceLevel);

    // Calculate the sample size using the formula
    int sampleSize = calculateSampleSize(zScore, marginOfError, populationProportion, totalRows);

    // Calculate the desired sampling rate
    double samplingRate = (double) sampleSize / totalRows;
    LOG.debug("Target sampling rate: " + samplingRate);

    // Calculate the number of bits to check in the mask, use floor to generate the largest sample
    int bitsToCheck = (int) Math.floor(Math.log(1 / samplingRate) / Math.log(2));

    // Generate the mask
    return (1 << bitsToCheck) - 1;
  }

  // Helper function to calculate the Z-score for a given confidence level
  private static double calculateZScore(Double confidenceLevel) {
    // This is a simplified Z-score calculation
    if (confidenceLevel == 0.90) {
      return 1.645;
    } else if (confidenceLevel == 0.95) {
      return 1.96;
    } else if (confidenceLevel == 0.99) {
      return 2.576;
    } else {
      throw new IllegalArgumentException(
          "Unsupported confidence level: "
              + confidenceLevel
              + ". Supported levels are [0.90, 0.95, 0.99]");
    }
  }

  private static int calculateSampleSize(
      double zScore, double marginOfError, double populationProportion, Long totalRows) {
    // Formula based on
    // https://www.evalacademy.com/articles/finding-the-right-sample-size-the-hard-way
    double numerator = Math.pow(zScore, 2) * populationProportion * (1 - populationProportion);
    double denominator = Math.pow(marginOfError, 2);
    double sampleSize = numerator / denominator;
    double finiteDenominator = 1 + (numerator / (denominator * totalRows));

    double finiteSample = sampleSize / finiteDenominator;
    LOG.debug("Target sample size:" + finiteSample);

    return (int) Math.ceil(finiteSample);
  }

  @Override
  public String getRowSampleQuery() {
    DSLContext dsl = getDSLContext();

    final double marginOfError = 0.05;
    final double populationProportion = 0.5;

    int mask = generateMask(getRowCount(), confidenceInterval, marginOfError, populationProportion);

    String sql =
        dsl.select()
            .from(getTable())
            .where(bitAnd(field(name(getPrimaryKey()), Integer.class), val(mask)).eq(0))
            .getSQL(ParamType.INLINED);

    LOG.debug("Row sample query generated: " + sql);
    return sql;
  }

  public String generateRowCountQuery() {
    DSLContext dsl = getDSLContext();
    return dsl.select(count()).getSQL();
  }
}
