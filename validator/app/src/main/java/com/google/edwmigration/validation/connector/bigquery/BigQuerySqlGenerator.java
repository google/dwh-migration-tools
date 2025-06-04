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
package com.google.edwmigration.validation.connector.bigquery;

import static org.jooq.impl.DSL.*;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.validation.config.BqTargetTable;
import com.google.edwmigration.validation.config.SourceTable;
import com.google.edwmigration.validation.model.UserInputContext;
import com.google.edwmigration.validation.sql.AbstractSqlGenerator;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySqlGenerator extends AbstractSqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySqlGenerator.class);

  private final SourceTable sourceTable;
  private final BqTargetTable targetTable;

  public BigQuerySqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull UserInputContext context,
      @Nonnull ImmutableMap<String, String> columnMappings) {
    super(dialect, context, columnMappings);
    this.sourceTable = context.sourceTable;
    this.targetTable = context.bqTargetTable;
  }

  private static final Map<String, DataType<? extends Number>> typeMappings = new HashMap<>();

  static {
    typeMappings.put("INT64", SQLDataType.BIGINT);
    typeMappings.put("NUMERIC", SQLDataType.NUMERIC.precision(38));
    typeMappings.put("BIGNUMERIC", SQLDataType.DECIMAL.precision(76));
    typeMappings.put("FLOAT64", SQLDataType.DOUBLE);
  }

  @Override
  public DataType<? extends Number> getSqlDataType(
      String dataType, Integer precision, Integer scale) {
    return typeMappings.get(dataType);
  }

  public String getNumericColumnsQuery(BqTargetTable table) {
    // TODO move to validation
    if (table.schema == null) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid BigQuery target table %s. Please provide `dataset.tableName`.", table.name));
    }

    SelectConditionStep<Record2<Object, Object>> query =
        getDSLContext()
            .select(field("column_name"), field("data_type"))
            .from(table(name(table.schema, "INFORMATION_SCHEMA", "COLUMNS")))
            .where(field(name("table_schema"), SQLDataType.VARCHAR).eq(table.schema))
            .and(field(name("table_name"), SQLDataType.VARCHAR).eq(table.name))
            .and(
                field(name("data_type"), SQLDataType.VARCHAR)
                    .in(val("INT64"), val("NUMERIC"), val("BIGNUMERIC"), val("FLOAT64")));

    String sql = query.getSQL(ParamType.INLINED);
    LOG.debug("Metadata query generated: {}", sql);
    return sql;
  }

  @Override
  public Long getRowCount() {
    // Replace with actual implementation
    return 49L;
  }

  public SourceTable getSourceTable() {
    return sourceTable;
  }

  public BqTargetTable getBqTargetTable() {
    return targetTable;
  }
}
