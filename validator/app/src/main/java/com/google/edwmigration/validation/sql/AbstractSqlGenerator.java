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
import com.google.edwmigration.validation.connector.api.SqlGenerator;
import com.google.edwmigration.validation.model.UserInputContext;
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
  private final ImmutableMap<String, String> columnMappings;

  public AbstractSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull UserInputContext context,
      @Nonnull ImmutableMap<String, String> columnMappings) {
    this.create = DSL.using(dialect);
    this.columnMappings = columnMappings;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  public ImmutableMap<String, String> getColumnMappings() {
    return columnMappings;
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
    //   final double confidence = 0.99;
    //   final double minDiscrepancyRate = 0.0001;

    //   long totalRows = getRowCount();
    //   long sampleSize = SamplingHelper.calculateDetectionSampleSize(confidence,
    // minDiscrepancyRate);
    //   double samplingRate = sampleSize < totalRows ? (double) sampleSize / totalRows : 1.0;
    //   int threshold = (int) Math.ceil(samplingRate * 10000);

    //   Name pkCol = name(pkColName);
    //   Field<Long> pkField = field(pkCol, Long.class); // numeric PK assumed
    //   Field<Integer> modExpr = pkField.mod(10000);

    String sql =
        create
            .select()
            .from(table(qualifiedTableName))
            //   .where(modExpr.lessThan(threshold))
            .getSQL(ParamType.INLINED);

    LOG.debug("Row sample query generated: {}", sql);
    return sql;
  }
}
