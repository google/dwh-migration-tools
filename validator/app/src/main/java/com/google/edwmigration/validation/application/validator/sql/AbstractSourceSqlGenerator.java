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
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
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
  private final ImmutableMap<String, String> columnMappings;

  public AbstractSourceSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull String table,
      @Nonnull Double confidenceInterval,
      @Nonnull ImmutableMap<String, String> columnMappings) {
    this.create = DSL.using(dialect);
    this.table = table;
    this.confidenceInterval = confidenceInterval;
    this.columnMappings = columnMappings;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  public String getTable() {
    return table;
  }

  public ImmutableMap<String, String> getColumnMappings() {
    return columnMappings;
  }

  @ForOverride
  public abstract Long getRowCount();

  @ForOverride
  public abstract String getPrimaryKey();

  @Override
  public String getAggregateQuery(HashMap<String, DataType<? extends Number>> numericColumns) {
    Table<?> table = table(getTable());
    Select<Record4<String, String, String, BigDecimal>> finalSelect = null;

    for (Map.Entry<String, DataType<? extends Number>> entry : numericColumns.entrySet()) {
      String sourceColumnName = entry.getKey();
      String targetColumnName =
          getColumnMappings().getOrDefault(sourceColumnName, sourceColumnName);

      DataType<? extends Number> columnType = entry.getValue();

      Field<BigDecimal> column = field(name(sourceColumnName), columnType).cast(BigDecimal.class);

      SelectJoinStep<Record4<String, String, String, BigDecimal>> sumSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val("sum").as("validation_type"),
                  sum(column).as("value"))
              .from(table);
      SelectJoinStep<Record4<String, String, String, BigDecimal>> minSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val("min").as("validation_type"),
                  min(column).as("value"))
              .from(table);
      SelectJoinStep<Record4<String, String, String, BigDecimal>> maxSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val("max").as("validation_type"),
                  max(column).as("value"))
              .from(table);
      SelectJoinStep<Record4<String, String, String, BigDecimal>> avgSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val("avg").as("validation_type"),
                  avg(column).cast(BigDecimal.class).as("value"))
              .from(table);
      Select<Record4<String, String, String, BigDecimal>> subselect =
          sumSelect.unionAll(minSelect).unionAll(maxSelect).unionAll(avgSelect);
      if (finalSelect == null) {
        finalSelect = subselect;
      } else {
        finalSelect = finalSelect.unionAll(subselect);
      }
    }

    if (finalSelect != null) {
      return finalSelect.getSQL(ParamType.INLINED);
    }
    return null;
  }

  private static int getModCondition(
      Long totalRows, Double confidenceLevel, double marginOfError, double populationProportion) {
    // Calculate the Z-score for the given confidence level
    double zScore = calculateZScore(confidenceLevel);

    // Calculate the sample size using the formula
    long sampleSize = calculateSampleSize(zScore, marginOfError, populationProportion, totalRows);

    // Calculate the desired sampling rate
    double samplingRate = (double) sampleSize / totalRows;
    LOG.debug("Target sampling rate: " + samplingRate);

    // Return int for mod condition
    return (int) Math.ceil(samplingRate * 100);
  }

  // Helper function to calculate the Z-score for a given confidence level
  private static double calculateZScore(Double confidenceLevel) {
    // This is a simplified Z-score calculation
    if (confidenceLevel == 90) {
      return 1.645;
    } else if (confidenceLevel == 95) {
      return 1.96;
    } else if (confidenceLevel == 99) {
      return 2.576;
    } else {
      throw new IllegalArgumentException(
          "Unsupported confidence level: "
              + confidenceLevel
              + ". Supported levels are [0.90, 0.95, 0.99]");
    }
  }

  private static long calculateSampleSize(
      double zScore, double marginOfError, double populationProportion, Long totalRows) {
    // Formula based on
    // https://www.evalacademy.com/articles/finding-the-right-sample-size-the-hard-way
    double numerator = Math.pow(zScore, 2) * populationProportion * (1 - populationProportion);
    double denominator = Math.pow(marginOfError, 2);
    double sampleSize = numerator / denominator;
    double finiteDenominator = 1 + (numerator / (denominator * totalRows));

    double finiteSample = sampleSize / finiteDenominator;
    LOG.debug("Target sample size:" + finiteSample);

    return (long) Math.ceil(finiteSample);
  }

  @Override
  public String getRowSampleQuery() {
    DSLContext dsl = getDSLContext();

    final double marginOfError = 0.05;
    final double populationProportion = 0.5;

    int modCondition =
        getModCondition(getRowCount(), confidenceInterval, marginOfError, populationProportion);

    String sql =
        dsl.select()
            .from(getTable())
            .where(
                (field(name(getPrimaryKey()), Integer.class).mod(val(100))).lessThan(modCondition))
            .getSQL(ParamType.INLINED);

    LOG.debug("Row sample query generated: " + sql);
    return sql;
  }
}
