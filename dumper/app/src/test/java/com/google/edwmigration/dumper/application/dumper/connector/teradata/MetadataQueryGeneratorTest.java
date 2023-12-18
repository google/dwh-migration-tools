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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.DBC_INFO_QUERY;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.createSelectForAllTempTablesVX;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.MetadataQueryGenerator.createSelectForTableTextV;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.eq;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.identifier;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.in;
import static com.google.edwmigration.dumper.application.dumper.connector.teradata.query.TeradataSelectBuilder.stringLiteral;
import static com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils.assertQueryEquals;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataQueryGeneratorTest {

  @Test
  public void createSelectForDatabasesV_noLimits() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.empty(),
            /* dbRows= */ OptionalLong.empty(),
            /* condition= */ Optional.empty());

    assertQueryEquals("SELECT %s FROM DBC.DatabasesV", query);
  }

  @Test
  public void createSelectForDatabasesV_condition() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.empty(),
            /* dbRows= */ OptionalLong.empty(),
            /* condition= */ Optional.of(eq(identifier("DatabaseName"), stringLiteral("abc"))));

    assertQueryEquals("SELECT %s FROM DBC.DatabasesV WHERE DatabaseName = 'abc'", query);
  }

  @Test
  public void createSelectForDatabasesV_usersLimit() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.of(13),
            /* dbRows= */ OptionalLong.empty(),
            /* condition= */ Optional.empty());

    assertQueryEquals(
        "SELECT %s FROM ("
            + " SELECT * FROM (SELECT TOP 13 * FROM DBC.DatabasesV"
            + "   WHERE DBKind = 'U' ORDER BY PermSpace DESC) AS users"
            + " UNION ALL "
            + " SELECT * FROM (SELECT * FROM DBC.DatabasesV WHERE DBKind = 'D') AS dbs"
            + ") AS t",
        query);
  }

  @Test
  public void createSelectForDatabasesV_usersLimitAndCondition() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.of(13),
            /* dbRows= */ OptionalLong.empty(),
            /* condition= */ Optional.of(eq(identifier("DatabaseName"), stringLiteral("abc"))));

    assertQueryEquals(
        "SELECT %s FROM ("
            + " SELECT * FROM (SELECT TOP 13 * FROM DBC.DatabasesV"
            + "   WHERE (DatabaseName = 'abc' AND DBKind = 'U') ORDER BY PermSpace DESC) AS users"
            + " UNION ALL "
            + " SELECT * FROM (SELECT * FROM DBC.DatabasesV"
            + "   WHERE (DatabaseName = 'abc' AND DBKind = 'D')) AS dbs"
            + ") AS t",
        query);
  }

  @Test
  public void createSelectForDatabasesV_dbsLimit() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.empty(),
            /* dbRows= */ OptionalLong.of(18),
            /* condition= */ Optional.empty());

    assertQueryEquals(
        "SELECT %s FROM ("
            + " SELECT * FROM (SELECT * FROM DBC.DatabasesV WHERE DBKind = 'U') AS users"
            + " UNION ALL "
            + " SELECT * FROM (SELECT TOP 18 * FROM DBC.DatabasesV"
            + "   WHERE DBKind = 'D' ORDER BY PermSpace DESC) AS dbs"
            + ") AS t",
        query);
  }

  @Test
  public void createSelectForDatabasesV_usersAndDbsLimit() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.of(15),
            /* dbRows= */ OptionalLong.of(18),
            /* condition= */ Optional.empty());

    assertQueryEquals(
        "SELECT %s FROM ("
            + " SELECT * FROM (SELECT TOP 15 * FROM DBC.DatabasesV"
            + "   WHERE DBKind = 'U' ORDER BY PermSpace DESC) AS users"
            + " UNION ALL "
            + " SELECT * FROM (SELECT TOP 18 * FROM DBC.DatabasesV"
            + "   WHERE DBKind = 'D' ORDER BY PermSpace DESC) AS dbs"
            + ") AS t",
        query);
  }

  @Test
  public void createSelectForDatabasesV_usersAndDbsLimitAndCondition() {
    String query =
        MetadataQueryGenerator.createSelectForDatabasesV(
            /* userRows= */ OptionalLong.of(15),
            /* dbRows= */ OptionalLong.of(18),
            /* condition= */ Optional.of(
                in(identifier("DatabaseName"), ImmutableList.of("db1", "db2"))));

    assertQueryEquals(
        "SELECT %s FROM ("
            + " SELECT * FROM (SELECT TOP 15 * FROM DBC.DatabasesV"
            + "   WHERE (DatabaseName IN ('db1', 'db2') AND DBKind = 'U')"
            + "   ORDER BY PermSpace DESC) AS users"
            + " UNION ALL "
            + " SELECT * FROM (SELECT TOP 18 * FROM DBC.DatabasesV"
            + "   WHERE (DatabaseName IN ('db1', 'db2') AND DBKind = 'D')"
            + "   ORDER BY PermSpace DESC) AS dbs"
            + ") AS t",
        query);
  }

  @Test
  public void dbcInfoQuery() {
    assertQueryEquals(
        "SELECT 'teradata' AS dialect, InfoData AS version, CURRENT_TIMESTAMP AS export_time"
            + " FROM dbc.dbcinfo WHERE InfoKey = 'VERSION'",
        DBC_INFO_QUERY);
  }

  @Test
  public void createSimpleSelect_success() {
    String query =
        MetadataQueryGenerator.createSimpleSelect("SampleTable", /* condition= */ Optional.empty());

    assertQueryEquals("SELECT %s FROM SampleTable", query);
  }

  @Test
  public void createSimpleSelect_withCondition() {
    String query =
        MetadataQueryGenerator.createSimpleSelect(
            "SampleTable", Optional.of(eq(identifier("col_a"), stringLiteral("abc"))));

    assertQueryEquals("SELECT %s FROM SampleTable WHERE col_a = 'abc'", query);
  }

  @Test
  public void createSelectForAllTempTablesVX_success() {
    String query = createSelectForAllTempTablesVX(/* databases= */ ImmutableList.of());
    assertQueryEquals("SELECT %s FROM DBC.AllTempTablesVX", query);
  }

  @Test
  public void createSelectForAllTempTablesVX_withDatabaseFiltering_success() {
    String query = createSelectForAllTempTablesVX(ImmutableList.of("db1", "db2"));
    assertQueryEquals(
        "SELECT %s FROM DBC.AllTempTablesVX WHERE B_DatabaseName IN ('DB1', 'DB2')", query);
  }

  @Test
  public void createSelectForTableTextV_success() {
    String query =
        createSelectForTableTextV(
            /* textMaxLength= */ OptionalLong.empty(), /* condition= */ Optional.empty());
    assertQueryEquals("SELECT %s FROM DBC.TableTextV", query);
  }

  @Test
  public void createSelectForTableTextV_withCondition() {
    // Act
    String query =
        createSelectForTableTextV(
            /* textMaxLength= */ OptionalLong.empty(),
            /* condition= */ Optional.of(
                eq(identifier("DatabaseName"), stringLiteral("sample_db"))));

    // Assert
    assertQueryEquals("SELECT %s FROM DBC.TableTextV WHERE DatabaseName = 'sample_db'", query);
  }

  @Test
  public void createSelectForTableTextV_withTextMaxLength() {
    // Act
    String query =
        createSelectForTableTextV(
            /* textMaxLength= */ OptionalLong.of(20000), /* condition= */ Optional.empty());

    // Assert
    assertQueryEquals(
        "SELECT DataBaseName, TableName, TableKind,"
            + "   CAST(SUBSTR(RequestText, 1, 20000) AS VARCHAR(20000)) AS RequestText,"
            + "   (((LineNo - 1) * 2) + 1) AS LineNo FROM DBC.TableTextV"
            + " UNION ALL"
            + " SELECT DataBaseName, TableName, TableKind,"
            + "   CAST(SUBSTR(RequestText, 20001, 20000) AS VARCHAR(20000)) AS RequestText,"
            + "   (((LineNo - 1) * 2) + 2) AS LineNo FROM DBC.TableTextV",
        query);
  }

  @Test
  public void createSelectForTableTextV_withTextMaxLengthAndCondition() {
    // Act
    String query =
        createSelectForTableTextV(
            /* textMaxLength= */ OptionalLong.of(20000),
            /* condition= */ Optional.of(
                eq(identifier("DatabaseName"), stringLiteral("sample_db"))));

    // Assert
    assertQueryEquals(
        "SELECT DataBaseName, TableName, TableKind,"
            + "   CAST(SUBSTR(RequestText, 1, 20000) AS VARCHAR(20000)) AS RequestText,"
            + "   (((LineNo - 1) * 2) + 1) AS LineNo FROM DBC.TableTextV"
            + "   WHERE DatabaseName = 'sample_db'"
            + " UNION ALL"
            + " SELECT DataBaseName, TableName, TableKind,"
            + "   CAST(SUBSTR(RequestText, 20001, 20000) AS VARCHAR(20000)) AS RequestText,"
            + "   (((LineNo - 1) * 2) + 2) AS LineNo FROM DBC.TableTextV"
            + "   WHERE DatabaseName = 'sample_db'",
        query);
  }
}
