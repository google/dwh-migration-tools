/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataUtils.optionalIf;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.and;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.eq;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.in;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.projection;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.selectAll;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.star;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.stringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.subquery;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.select;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.selectTop;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.union;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.Expression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.OrderBySpec;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.OrderBySpec.Direction;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression;
import com.google.edwmigration.dumper.application.dumper.connector.teradata.query.model.SelectExpression.FromClauseStepBuilder;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Generator of queries used during dumping metadata from Teradata. */
class MetadataQueryGenerator {

  /** The length of the VARCHAR column {@code TableTextV.RequestText}. */
  static final int TABLE_TEXT_V_REQUEST_TEXT_LENGTH = 32000;

  static final String DBC_INFO_QUERY =
      select(
              stringLiteral("teradata").as("dialect"),
              identifier("InfoData").as("version"),
              identifier("CURRENT_TIMESTAMP").as("export_time"))
          .from("dbc.dbcinfo")
          .where(eq(identifier("InfoKey"), stringLiteral("VERSION")))
          .serialize();

  static String createSelectForDatabasesV(
      OptionalLong userRows, OptionalLong dbRows, Optional<Expression> condition) {
    if (!userRows.isPresent() && !dbRows.isPresent()) {
      return createSimpleSelect("DBC.DatabasesV", condition);
    }
    SelectExpression usersSelect = createSingleDbKindSelectFromDatabasesV("U", userRows, condition);
    SelectExpression dbsSelect = createSingleDbKindSelectFromDatabasesV("D", dbRows, condition);
    return select("%s")
        .from(
            subquery(
                union(
                    selectAll().from(subquery(usersSelect)).as("users").build(),
                    selectAll().from(subquery(dbsSelect)).as("dbs").build())))
        .as("t")
        .serialize();
  }

  static String createSimpleSelect(String tableName, Optional<Expression> condition) {
    FromClauseStepBuilder partialQuery = select("%s").from(tableName);
    return condition.map(partialQuery::where).orElse(partialQuery).serialize();
  }

  static String createSelectForAllTempTablesVX(List<String> databases) {
    return createSimpleSelect(
        "DBC.AllTempTablesVX",
        optionalIf(
            !databases.isEmpty(),
            () ->
                in(
                    identifier("B_DatabaseName"),
                    databases.stream().map(String::toUpperCase).collect(toList()))));
  }

  static String createSelectForTableTextV(
      OptionalLong textMaxLength, Optional<Expression> condition) {
    String tableName = "DBC.TableTextV";
    if (!textMaxLength.isPresent()) {
      return createSimpleSelect(tableName, condition);
    }
    int splitTextColumnMaxLength = Ints.checkedCast(textMaxLength.getAsLong());
    return new SplitTextColumnQueryGenerator(
            ImmutableList.of("DataBaseName", "TableName", "TableKind"),
            "RequestText",
            "LineNo",
            tableName,
            condition,
            TABLE_TEXT_V_REQUEST_TEXT_LENGTH,
            splitTextColumnMaxLength)
        .generate();
  }

  private static SelectExpression createSingleDbKindSelectFromDatabasesV(
      String dbKind, OptionalLong rowCount, Optional<Expression> condition) {
    String tableName = "DBC.DatabasesV";
    Expression dbKindCondition = eq(identifier("DBKind"), stringLiteral(dbKind));
    Expression processedCondition =
        condition
            .<Expression>map(innerCondition -> and(innerCondition, dbKindCondition))
            .orElse(dbKindCondition);
    if (rowCount.isPresent()) {
      return selectTop(rowCount.getAsLong(), projection(star()))
          .from(tableName)
          .where(processedCondition)
          .orderBy(OrderBySpec.create(identifier("PermSpace"), Direction.DESC));
    } else {
      return selectAll().from(tableName).where(processedCondition).build();
    }
  }
}
