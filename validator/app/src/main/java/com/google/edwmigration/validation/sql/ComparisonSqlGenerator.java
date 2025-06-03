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

import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.stddevSamp;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.val;
import static org.jooq.impl.DSL.when;
import static org.jooq.tools.StringUtils.firstNonNull;

import com.google.edwmigration.validation.NameManager;
import com.google.edwmigration.validation.NameManager.ValidationType;
import com.google.edwmigration.validation.ValidationColumnMapping;
import com.google.edwmigration.validation.ValidationColumnMapping.ColumnEntry;
import com.google.edwmigration.validation.ValidationTableMapping;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Record9;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectOnConditionStep;
import org.jooq.SelectOrderByStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ComparisonSqlGenerator implements SqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonSqlGenerator.class);
  private final DSLContext create;
  private final ValidationTableMapping validationTableMapping;

  private final NameManager nameManager;
  private String startTime = null;
  private String runId = null;

  public ComparisonSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull ValidationTableMapping validationTableMapping,
      @Nonnull NameManager nameManager) {
    this.create = DSL.using(dialect);
    this.validationTableMapping = validationTableMapping;
    this.nameManager = nameManager;
  }

  public NameManager getNameManager() {
    return nameManager;
  }

  @Override
  public DSLContext getDSLContext() {
    return create;
  }

  @Override
  public ValidationTableMapping getValidationTableMapping() {
    return validationTableMapping;
  }

  private String getRunId() {
    if (runId == null) {
      UUID uuid = UUID.randomUUID();
      runId = uuid.toString();
    }
    return runId;
  }

  private String getStartTime() {
    if (startTime == null) {
      startTime = Instant.now().toString();
    }
    return startTime;
  }

  static Map<String, DataType<?>> typeMappings = new HashMap<>();
  static Set<DataType<?>> numericTypes =
      new HashSet<>(
          Arrays.asList(
              SQLDataType.BIGINT,
              SQLDataType.NUMERIC.precision(38),
              SQLDataType.DECIMAL.precision(76),
              SQLDataType.DOUBLE));

  static {
    typeMappings.put("INT64", SQLDataType.BIGINT);
    typeMappings.put("NUMERIC", SQLDataType.NUMERIC.precision((38)));
    typeMappings.put("BIGNUMERIC", SQLDataType.DECIMAL.precision(76));
    typeMappings.put("FLOAT64", SQLDataType.DOUBLE);
    typeMappings.put("STRING", SQLDataType.VARCHAR);
    typeMappings.put("TIMESTAMP", SQLDataType.TIMESTAMP);
    typeMappings.put("TIME", SQLDataType.TIME);
    typeMappings.put("DATE", SQLDataType.DATE);
    typeMappings.put("DATETIME", SQLDataType.TIMESTAMP);
    typeMappings.put("BOOLEAN", SQLDataType.BOOLEAN);
  }

  public DataType<?> getSqlDataType(String dataType) {
    return typeMappings.get(dataType);
  }

  public String getAggregateCompareQuery() {
    String sourceTableName = getValidationTableMapping().getSourceTable().getFullyQualifiedTable();
    String targetTableName = getValidationTableMapping().getTargetTable().getFullyQualifiedTable();

    String aggSourceTable =
        getNameManager().getFullyQualifiedBqSourceTableName(ValidationType.AGGREGATE);
    String aggTargetTable =
        getNameManager().getFullyQualifiedBqTargetTableName(ValidationType.AGGREGATE);

    Field<String> s_source_column_name = field(name("s", "source_column_name"), String.class);
    Field<String> t_target_column_name = field(name("t", "target_column_name"), String.class);
    Field<String> s_validation_type = field(name("s", "validation_type"), String.class);
    Field<String> s_target_column_name = field(name("s", "target_column_name"), String.class);
    Field<BigDecimal> s_value = field(name("s", "value"), SQLDataType.NUMERIC);
    Field<BigDecimal> t_value = field(name("t", "value"), SQLDataType.NUMERIC);

    String result;
    try (SelectOnConditionStep<
            Record9<
                String, Timestamp, String, String, String, String, String, BigDecimal, BigDecimal>>
        query =
            getDSLContext()
                .select(
                    val(getRunId()).as("run_id"),
                    DSL.function("TIMESTAMP", Timestamp.class, val(getStartTime()))
                        .as("start_time"),
                    s_source_column_name.as("source_column_name"),
                    t_target_column_name.as("target_column_name"),
                    s_validation_type.as("validation_type"),
                    val(sourceTableName).as("source_table_name"),
                    val(targetTableName).as("target_table_name"),
                    s_value.as("source_agg_value"),
                    t_value.as("target_agg_value"))
                .from(table(aggSourceTable).as("s"))
                .fullOuterJoin(table(aggTargetTable).as("t"))
                .on(
                    s_source_column_name.eq(field(name("t", "source_column_name"), String.class)),
                    s_validation_type.eq(field(name("t", "validation_type"), String.class)),
                    s_target_column_name.eq(
                        field(name("t", "target_column_name"), String.class)))) {

      result = query.getSQL(ParamType.INLINED);
    }

    LOG.debug("Aggregate comparison query generated: " + result);
    return result;
  }

  public String getColumnMetadataQuery(String table) {
    String schema = getNameManager().getDataset();
    if (schema == null) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid BigQuery target table %s. Please provide `dataset.tableName`.", table));
    }

    SelectConditionStep<Record2<Object, Object>> query =
        getDSLContext()
            .select(field("column_name"), field("data_type"))
            .from(table(name(schema, "INFORMATION_SCHEMA", "COLUMNS")))
            .where(field(name("table_schema"), SQLDataType.VARCHAR).eq(schema))
            .and(field(name("table_name"), SQLDataType.VARCHAR).eq(table));

    String inlinedQuery = query.getSQL(ParamType.INLINED);

    LOG.debug("Metadata query generated: " + inlinedQuery);
    return inlinedQuery;
  }

  private <T> Condition buildJoinCondition(
      Condition joinCondition,
      String sourcePk,
      DataType<?> sourcePkDatatype,
      String targetPk,
      DataType<?> targetPkDataType) {

    Field<T> sourcePkField = (Field<T>) field(name("s", sourcePk), sourcePkDatatype);
    Field<T> targetPkField = (Field<T>) field(name("t", targetPk), targetPkDataType);

    if (joinCondition == null) {
      joinCondition = sourcePkField.eq(targetPkField);
    } else {
      joinCondition = joinCondition.and(sourcePkField.eq(targetPkField));
    }

    return joinCondition;
  }

  public String getRowCompareQuery(ValidationColumnMapping validationColumnMapping) {
    String sourceTableName = getValidationTableMapping().getSourceTable().getFullyQualifiedTable();
    String targetTableName = getValidationTableMapping().getTargetTable().getFullyQualifiedTable();

    String rowSourceTable = getNameManager().getFullyQualifiedBqSourceTableName(ValidationType.ROW);
    String rowTargetTable = getNameManager().getFullyQualifiedBqTargetTableName(ValidationType.ROW);

    Set<Field<?>> joinedFields = new HashSet<>();
    Condition joinCondition = null;

    for (ColumnEntry colEntry : validationColumnMapping.getColumnEntries()) {
      if (colEntry.getSourceColumnName() != null) {
        Field<?> sourceField =
            field(name("s", colEntry.getSourceColumnName()), colEntry.getSourceColumnDataType())
                .as(colEntry.getSourceColumnAlias());
        joinedFields.add(sourceField);
      }
      if (colEntry.getTargetColumnName() != null) {
        Field<?> targetField =
            field(name("t", colEntry.getTargetColumnName()), colEntry.getTargetColumnDataType())
                .as(colEntry.getTargetColumnAlias());
        joinedFields.add(targetField);
      }
      if (colEntry.isPrimaryKey()) {
        joinCondition =
            buildJoinCondition(
                joinCondition,
                colEntry.getSourceColumnName(),
                colEntry.getSourceColumnDataType(),
                colEntry.getTargetColumnName(),
                colEntry.getTargetColumnDataType());
      }
    }

    CommonTableExpression<Record> joinedCte =
        name("joined")
            .as(
                select(joinedFields)
                    .from(table(rowSourceTable).as("s"))
                    .fullOuterJoin(table(rowTargetTable).as("t"))
                    .on(joinCondition));

    SelectOrderByStep<Record> finalQuery = null;

    for (ColumnEntry colEntry : validationColumnMapping.getColumnEntries()) {
      Set<Field<?>> selectFields =
          new HashSet<>(
              Arrays.asList(
                  val(getRunId()).as("run_id"),
                  DSL.function("TIMESTAMP", Timestamp.class, val(getStartTime())).as("start_time"),
                  val(colEntry.getSourceColumnName()).as("source_column_name"),
                  val(colEntry.getTargetColumnName()).as("target_column_name"),
                  val("row").as("validation_type"),
                  val(sourceTableName).as("source_table_name"),
                  val(targetTableName).as("target_table_name"),
                  count(field(name(colEntry.getSourceColumnAlias())))
                      .cast(BigDecimal.class)
                      .as("source_agg_value"),
                  count(field(name(colEntry.getTargetColumnAlias())))
                      .cast(BigDecimal.class)
                      .as("target_agg_value")));

      if (colEntry.getSourceColumnName() != null && colEntry.getTargetColumnName() != null) {

        Field<BigDecimal> difference;
        if (numericTypes.contains(colEntry.getSourceColumnDataType())) {
          difference =
              field(name(colEntry.getSourceColumnAlias()), BigDecimal.class)
                  .sub(field(name(colEntry.getTargetColumnAlias()), BigDecimal.class));
        } else if (colEntry.getSourceColumnDataType() == SQLDataType.VARCHAR) {
          difference =
              DSL.function(
                  "EDIT_DISTANCE",
                  SQLDataType.NUMERIC,
                  field(name(colEntry.getSourceColumnAlias())),
                  field(name(colEntry.getTargetColumnAlias())));
        } else if (colEntry.getSourceColumnDataType() == SQLDataType.TIMESTAMP) {
          Field<Long> epochSource =
              DSL.function(
                  "UNIX_SECONDS", Long.class, field(name(colEntry.getSourceColumnAlias())));
          Field<Long> epochTarget =
              DSL.function(
                  "UNIX_SECONDS", Long.class, field(name(colEntry.getTargetColumnAlias())));
          difference = epochSource.minus(epochTarget).cast(BigDecimal.class);
        } else if (colEntry.getSourceColumnDataType() == SQLDataType.TIME) {
          Field<Long> epochSource =
              DSL.function(
                  "UNIX_SECONDS",
                  Long.class,
                  field(name(colEntry.getSourceColumnAlias())).cast(SQLDataType.TIMESTAMP));
          Field<Long> epochTarget =
              DSL.function(
                  "UNIX_SECONDS",
                  Long.class,
                  field(name(colEntry.getTargetColumnAlias())).cast(SQLDataType.TIMESTAMP));
          difference = epochSource.minus(epochTarget).cast(BigDecimal.class);
        } else if (colEntry.getSourceColumnDataType() == SQLDataType.DATE) {
          Field<Long> epochSource =
              DSL.function("UNIX_DATE", Long.class, field(name(colEntry.getSourceColumnAlias())));
          Field<Long> epochTarget =
              DSL.function("UNIX_DATE", Long.class, field(name(colEntry.getTargetColumnAlias())));
          difference = epochSource.minus(epochTarget).cast(BigDecimal.class);
        } else if (colEntry.getSourceColumnDataType() == SQLDataType.BOOLEAN) {
          Condition condition =
              field(name(colEntry.getSourceColumnAlias()))
                  .eq(field(name(colEntry.getTargetColumnAlias())));
          difference = DSL.when(condition, val(0)).otherwise(val(1)).cast(BigDecimal.class);
        } else {
          difference =
              field(name(colEntry.getSourceColumnAlias()), BigDecimal.class)
                  .sub(field(name(colEntry.getTargetColumnAlias()), BigDecimal.class));
        }

        Field<BigDecimal> countDiff =
            count(when(difference.gt(val(0, SQLDataType.NUMERIC)), val(1, SQLDataType.NUMERIC)))
                .cast(SQLDataType.NUMERIC)
                .as("count_differences");
        Field<BigDecimal> max = max(difference).as("max_differences");
        Field<BigDecimal> stddev =
            stddevSamp(difference).cast(BigDecimal.class).as("stddev_differences");

        selectFields.addAll(Arrays.asList(countDiff, max, stddev));
      } else {
        // TODO: Add additional logic to handle missing or extra columns.
        String colName =
            firstNonNull(colEntry.getSourceColumnName(), colEntry.getTargetColumnName());
        LOG.info(
            String.format("Missing or additional column %s detected. Skipping for now." + colName));
      }

      if (finalQuery == null) {
        finalQuery = getDSLContext().with(joinedCte).select(selectFields).from(joinedCte);
      } else {
        SelectJoinStep<Record> query = select(selectFields).from(joinedCte);

        finalQuery = finalQuery.unionAll(query);
      }
    }

    String inlinedQuery = finalQuery.getSQL(ParamType.INLINED);

    LOG.debug("Row level comparison query generated: " + inlinedQuery);
    return inlinedQuery;
  }
}
