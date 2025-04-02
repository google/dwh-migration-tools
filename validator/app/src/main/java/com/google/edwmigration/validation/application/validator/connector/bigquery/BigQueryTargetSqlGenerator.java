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
package com.google.edwmigration.validation.application.validator.connector.bigquery;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.val;

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.validation.application.validator.sql.AbstractSourceSqlGenerator;
import javax.annotation.Nonnull;
import org.jooq.DataType;
import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryTargetSqlGenerator extends AbstractSourceSqlGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTargetSqlGenerator.class);

  public BigQueryTargetSqlGenerator(
      @Nonnull SQLDialect dialect,
      @Nonnull String table,
      @Nonnull Double confidenceInterval,
      @Nonnull ImmutableMap<String, String> columnMappings) {
    super(dialect, table, confidenceInterval, columnMappings);
  }

  @Override
  public Long getRowCount() {
    return 49L;
  }

  @Override
  public String getPrimaryKey() {
    return "id";
  }

  // Only need this for Source SQL generation
  @Override
  public DataType<? extends Number> getSqlDataType(
      String dataType, Integer numericPrecision, Integer numericScale) {
    return null;
  }

  public String getNumericColumnsQuery() {
    String table;
    String schema;
    String[] tableName = getTable().split(".");
    if (tableName.length == 2) {
      schema = tableName[0];
      table = tableName[1];
    } else {
      throw new RuntimeException("Invalid BigQuery table. Provide dataset.table.");
    }

    SelectConditionStep<Record2<Object, Object>> query =
        getDSLContext()
            .select(field("column_name"), field("data_type"))
            .from(table(name("information_schema", "columns")))
            .where(field(name("table_schema"), SQLDataType.VARCHAR).eq(schema))
            .and(field(name("table_name"), SQLDataType.VARCHAR).eq(table))
            .and(field(name("data_type"), SQLDataType.VARCHAR).in(val("INT64")));
    String inlinedQuery = query.getSQL(ParamType.INLINED);

    LOG.debug("Metadata query generated: " + inlinedQuery);
    return inlinedQuery;
  }
}
