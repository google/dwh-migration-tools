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
package com.google.edwmigration.validation.application.validator.connector.postgresql;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.val;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.validation.application.validator.sql.AbstractSourceSqlGenerator;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.jooq.DataType;
import org.jooq.Record4;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class PostgresqlSourceSqlGenerator extends AbstractSourceSqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresqlSourceSqlGenerator.class);

  private static final String PG_DEFAULT_SCHEMA = "public";

  public PostgresqlSourceSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull String table,
      @Nonnull Double confidenceInterval,
      @Nonnull ImmutableMap<String, String> columnMappings) {
    super(dialect, table, confidenceInterval, columnMappings);
  }

  static Map<String, DataType<? extends Number>> typeMappings = new HashMap<>();

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

  public DataType<? extends Number> getSqlDataType(
      String dataType, Integer precision, Integer scale) {
    DataType<? extends Number> type = typeMappings.get(dataType);
    type.precision(precision);
    type.scale(scale);
    return type;
  }

  public String getNumericColumnsQuery() {
    String table = getTable();
    String schema = PG_DEFAULT_SCHEMA;
    String[] tableName = getTable().split(".");
    if (tableName.length == 2) {
      schema = tableName[0];
      table = tableName[1];
    }

    LOG.debug(
        String.format("Getting metadata for Postgresql schema %s and table %s", schema, table));

    SelectConditionStep<Record4<Object, Object, Object, Object>> query =
        getDSLContext()
            .select(
                field(name("column_name")),
                field(name("data_type")),
                field(name("numeric_precision")),
                field(name("numeric_scale")))
            .from(table(name("information_schema", "columns")))
            .where(field(name("table_schema"), SQLDataType.VARCHAR).eq(schema))
            .and(field(name("table_name"), SQLDataType.VARCHAR).eq(table))
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

    LOG.debug("Metadata query generated: " + inlinedQuery);
    return inlinedQuery;
  }

  public String getPrimaryKey() {

    return "id";
  }

  public Long getRowCount() {
    return 49L;
  }
}
