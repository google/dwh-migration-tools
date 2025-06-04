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
package com.google.edwmigration.validation.sql;

import static org.jooq.impl.DSL.*;

import com.google.common.collect.ImmutableMap;
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

public abstract class AbstractSqlGenerator implements SqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSqlGenerator.class);
  private final DSLContext create;
  private final Double confidenceInterval;
  private final ImmutableMap<String, String> columnMappings;
  private final ImmutableMap<String, String> primaryKeys;

  public AbstractSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull Double confidenceInterval,
      @Nonnull ImmutableMap<String, String> columnMappings,
      @Nonnull ImmutableMap<String, String> primaryKeys) {
    this.create = DSL.using(dialect);
    this.confidenceInterval = confidenceInterval;
    this.columnMappings = columnMappings;
    this.primaryKeys = primaryKeys;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  public ImmutableMap<String, String> getColumnMappings() {
    return columnMappings;
  }

  public ImmutableMap<String, String> getPrimaryKeys() {
    return primaryKeys;
  }

  public abstract Long getRowCount();

  public abstract DataType<? extends Number> getSqlDataType(
      String dataType, @Nullable Integer numericPrecision, @Nullable Integer numericScale);

  public String getAggregateQuery(
      HashMap<String, DataType<? extends Number>> numericColumns,
      String qualifiedTableName,
      String sourceTableName,
      String targetTableName,
      boolean isSource) {

    Table<?> jooqTable = table(qualifiedTableName);

    Select<Record6<String, String, String, String, String, BigDecimal>> finalSelect =
        create
            .select(
                val("_ALL_COLUMNS").as("source_column_name"),
                val("_ALL_COLUMNS").as("target_column_name"),
                val(sourceTableName).as("source_table_name"),
                val(targetTableName).as("target_table_name"),
                val("count_star").as("validation_type"),
                count().cast(BigDecimal.class).as("value"))
            .from(jooqTable);

    for (Map.Entry<String, DataType<? extends Number>> entry : numericColumns.entrySet()) {
      String sourceCol = entry.getKey();
      String targetCol = columnMappings.getOrDefault(sourceCol, sourceCol);
      DataType<? extends Number> columnType = entry.getValue();

      Name columnName = name(isSource ? sourceCol : targetCol);
      Field<BigDecimal> column = field(columnName, columnType).cast(BigDecimal.class);

      Select<Record6<String, String, String, String, String, BigDecimal>> sum =
          create
              .select(
                  val(sourceCol).as("source_column_name"),
                  val(targetCol).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("sum").as("validation_type"),
                  sum(column).as("value"))
              .from(jooqTable);

      Select<Record6<String, String, String, String, String, BigDecimal>> min =
          create
              .select(
                  val(sourceCol).as("source_column_name"),
                  val(targetCol).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("min").as("validation_type"),
                  min(column).as("value"))
              .from(jooqTable);

      Select<Record6<String, String, String, String, String, BigDecimal>> max =
          create
              .select(
                  val(sourceCol).as("source_column_name"),
                  val(targetCol).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("max").as("validation_type"),
                  max(column).as("value"))
              .from(jooqTable);

      Select<Record6<String, String, String, String, String, BigDecimal>> avg =
          create
              .select(
                  val(sourceCol).as("source_column_name"),
                  val(targetCol).as("target_column_name"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  val("avg").as("validation_type"),
                  avg(column).cast(BigDecimal.class).as("value"))
              .from(jooqTable);

      finalSelect = finalSelect.unionAll(sum).unionAll(min).unionAll(max).unionAll(avg);
    }

    String sql = finalSelect.getSQL(ParamType.INLINED);
    LOG.debug("Aggregate query generated: {}", sql);
    return sql;
  }

  public String getRowSampleQuery(String qualifiedTableName, String pkColName) {
    final double marginOfError = 0.05;
    final double populationProportion = 0.5;

    int modCondition =
        getModCondition(getRowCount(), confidenceInterval, marginOfError, populationProportion);

    Name pkCol = name(pkColName);

    String sql =
        create
            .select()
            .from(table(qualifiedTableName))
            .where(field(pkCol).mod(val(100)).lessThan(modCondition))
            .getSQL(ParamType.INLINED);

    LOG.debug("Row sample query generated: {}", sql);
    return sql;
  }

  private static int getModCondition(
      Long totalRows, Double confidence, double margin, double proportion) {
    double z;
    switch (confidence.intValue()) {
      case 90:
        z = 1.645;
        break;
      case 95:
        z = 1.96;
        break;
      case 99:
        z = 2.576;
        break;
      default:
        throw new IllegalArgumentException("Unsupported confidence: " + confidence);
    }

    double num = Math.pow(z, 2) * proportion * (1 - proportion);
    double denom = Math.pow(margin, 2);
    double finiteDenom = 1 + (num / (denom * totalRows));
    double sampleSize = (num / denom) / finiteDenom;
    double rate = sampleSize / totalRows;
    LOG.debug("Target sampling rate: {}", rate);
    return (int) Math.ceil(rate * 100);
  }
}
