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
import com.google.edwmigration.validation.application.validator.ValidationTableMapping;
import com.google.edwmigration.validation.application.validator.ValidationTableMapping.TableType;
import com.google.edwmigration.validation.application.validator.ValidationTableMapping.ValidationTable;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public abstract class AbstractSqlGenerator implements SqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSqlGenerator.class);
  private final DSLContext create;
  private final ValidationTableMapping validationTableMapping;
  private ValidationTable validationTable = null;
  private final Double confidenceInterval;
  private final TableType tableType;
  private final ImmutableMap<String, String> columnMappings;
  private final ImmutableMap<String, String> primaryKeys;

  public AbstractSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull ValidationTableMapping validationTableMapping,
      @Nonnull Double confidenceInterval,
      @Nonnull ImmutableMap<String, String> columnMappings,
      @Nonnull TableType tableType,
      @Nonnull ImmutableMap<String, String> primaryKeys) {
    this.create = DSL.using(dialect);
    this.validationTableMapping = validationTableMapping;
    this.confidenceInterval = confidenceInterval;
    this.columnMappings = columnMappings;
    this.tableType = tableType;
    this.primaryKeys = primaryKeys;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  public ValidationTable getValidationTable() {
    if (validationTable == null) {
      if (tableType == TableType.SOURCE) {
        validationTable = validationTableMapping.getSourceTable();
      } else {
        validationTable = validationTableMapping.getTargetTable();
      }
    }
    return validationTable;
  }

  public ImmutableMap<String, String> getColumnMappings() {
    return columnMappings;
  }

  public ValidationTableMapping getValidationTableMapping() {
    return validationTableMapping;
  }

  @ForOverride
  public abstract Long getRowCount();

  public ImmutableMap<String, String> getPrimaryKeys() {
    return primaryKeys;
  }

  @ForOverride
  public abstract DataType<? extends Number> getSqlDataType(
      String dataType, @Nullable Integer numericPrecision, @Nullable Integer numericScale);

  public String getAggregateQuery(HashMap<String, DataType<? extends Number>> numericColumns) {
    Table<?> table = table(getValidationTable().getFullyQualifiedTable());
    Select<Record6<String, String, String, String, String, BigDecimal>> finalSelect = null;

    String sourceTableName = getValidationTableMapping().getSourceTable().getFullyQualifiedTable();
    String targetTableName = getValidationTableMapping().getTargetTable().getFullyQualifiedTable();

    Select<Record6<String, String, String, String, String, BigDecimal>> countStar =
        getDSLContext()
            .select(
                val("_ALL_COLUMNS").as("source_column_name"),
                val("_ALL_COLUMNS").as("target_column_name"),
                val(sourceTableName).as("source_table_name"),
                val(targetTableName).as("target_table_name"),
                val("count_star").as("validation_type"),
                count().cast(BigDecimal.class).as("value"))
            .from(table);

    for (Map.Entry<String, DataType<? extends Number>> entry : numericColumns.entrySet()) {
      String sourceColumnName = entry.getKey();
      String targetColumnName =
          getColumnMappings().getOrDefault(sourceColumnName, sourceColumnName);

      DataType<? extends Number> columnType = entry.getValue();

      Name columnName =
          (getValidationTable().getTableType() == TableType.SOURCE)
              ? name(sourceColumnName)
              : name(targetColumnName);
      Field<BigDecimal> column = field(columnName, columnType).cast(BigDecimal.class);

      SelectJoinStep<Record6<String, String, String, String, String, BigDecimal>> sumSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("sum").as("validation_type"),
                  sum(column).as("value"))
              .from(table);
      SelectJoinStep<Record6<String, String, String, String, String, BigDecimal>> minSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("min").as("validation_type"),
                  min(column).as("value"))
              .from(table);
      SelectJoinStep<Record6<String, String, String, String, String, BigDecimal>> maxSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("max").as("validation_type"),
                  max(column).as("value"))
              .from(table);
      SelectJoinStep<Record6<String, String, String, String, String, BigDecimal>> avgSelect =
          getDSLContext()
              .select(
                  val(sourceColumnName).as("source_column_name"),
                  val(targetColumnName).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("avg").as("validation_type"),
                  avg(column).cast(BigDecimal.class).as("value"))
              .from(table);
      SelectOrderByStep<Record6<String, String, String, String, String, BigDecimal>> subselect =
          sumSelect.unionAll(minSelect).unionAll(maxSelect).unionAll(avgSelect);
      if (finalSelect == null) {
        finalSelect = countStar.unionAll(subselect);
      } else {
        finalSelect = finalSelect.unionAll(subselect);
      }
    }

    if (finalSelect != null) {
      String result = finalSelect.getSQL(ParamType.INLINED);
      LOG.debug("Aggregate query generated: " + result);
      return result;
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

  public String getRowSampleQuery() {
    DSLContext dsl = getDSLContext();

    final double marginOfError = 0.05;
    final double populationProportion = 0.5;

    int modCondition =
        getModCondition(getRowCount(), confidenceInterval, marginOfError, populationProportion);

    // First PK will be used for the mod operation.
    Map.Entry<String, String> pkEntry = getPrimaryKeys().entrySet().iterator().next();

    Name pkColumn =
        (getValidationTable().getTableType() == TableType.SOURCE)
            ? name(pkEntry.getKey())
            : name(pkEntry.getValue());

    String sql =
        dsl.select()
            .from(getValidationTable().getFullyQualifiedTable())
            .where((field(pkColumn).mod(val(100))).lessThan(modCondition))
            .getSQL(ParamType.INLINED);

    LOG.debug("Row sample query generated: " + sql);
    return sql;
  }
}
