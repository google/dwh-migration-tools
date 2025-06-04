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
package com.google.edwmigration.validation.connector.postgresql;

import static org.jooq.impl.DSL.*;

import com.google.common.collect.ImmutableMap;
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

public class PostgresqlSqlGenerator extends AbstractSqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlSqlGenerator.class);

  public PostgresqlSqlGenerator(@Nonnull SQLDialect dialect, @Nonnull UserInputContext context) {

    super(dialect, context, ImmutableMap.of());
  }

  private static final Map<String, DataType<? extends Number>> typeMappings = new HashMap<>();

  static {
    typeMappings.put("integer", SQLDataType.INTEGER);
    typeMappings.put("smallint", SQLDataType.SMALLINT);
    typeMappings.put("bigint", SQLDataType.BIGINT);
    typeMappings.put("decimal", SQLDataType.DECIMAL);
    typeMappings.put("numeric", SQLDataType.NUMERIC);
    typeMappings.put("real", SQLDataType.REAL);
    typeMappings.put("double precision", SQLDataType.DOUBLE);
    typeMappings.put("money", SQLDataType.DECIMAL);
  }

  @Override
  public DataType<? extends Number> getSqlDataType(
      String dataType, Integer precision, Integer scale) {
    DataType<? extends Number> type = typeMappings.get(dataType);
    if (type == null) {
      throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
    if (precision != null) type = type.precision(precision);
    if (scale != null) type = type.scale(scale);
    return type;
  }

  public String getNumericColumnsQuery(SourceTable table) {
    LOG.debug("Getting metadata for PostgreSQL schema {} and table {}", table.schema, table.name);

    SelectConditionStep<Record4<Object, Object, Object, Object>> query =
        getDSLContext()
            .select(
                field(name("column_name")),
                field(name("data_type")),
                field(name("numeric_precision")),
                field(name("numeric_scale")))
            .from(table(name("information_schema", "columns")))
            .where(field(name("table_schema"), SQLDataType.VARCHAR).eq(table.schema))
            .and(field(name("table_name"), SQLDataType.VARCHAR).eq(table.name))
            .and(
                field(name("data_type"), SQLDataType.VARCHAR)
                    .in(
                        val("smallint"),
                        val("integer"),
                        val("bigint"),
                        val("decimal"),
                        val("numeric"),
                        val("real"),
                        val("double precision"),
                        val("money")));

    String inlinedQuery = query.getSQL(ParamType.INLINED);
    LOG.debug("Metadata query generated: {}", inlinedQuery);
    return inlinedQuery;
  }

  @Override
  public Long getRowCount() {
    // Replace with actual implementation that returns the row count from PostgreSQL
    return 49L;
  }
}
