/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.integration;

import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.base.TestBase;
import com.google.edwmigration.dumper.pojo.PgTableDefRow;
import com.google.edwmigration.dumper.sql.SqlUtil;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PgTableDefTest extends TestBase {

  private static final String SQL_PATH_PRIVATE =
      SQL_REQUESTS_BASE_PATH + "pg_table_def_private.sql";
  private static final String CSV_FILE_PATH_PRIVATE =
      EXPORTED_FILES_BASE_PATH + "pg_table_def_private.csv";
  private static final String SQL_PATH_GENERIC =
      SQL_REQUESTS_BASE_PATH + "pg_table_def_generic.sql";
  private static final String CSV_FILE_PATH_GENERIC =
      EXPORTED_FILES_BASE_PATH + "pg_table_def_generic.csv";

  private static Connection connection;

  @BeforeClass
  public void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void pgTableDefPrivateTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<PgTableDefRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<PgTableDefRow> csvList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SqlUtil.getSql(SQL_PATH_PRIVATE))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbList.add(PgTableDefRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH_PRIVATE);
    try (CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        PgTableDefRow csvRow = PgTableDefRow.create(line);
        csvList.add(csvRow);
      }
    }

    LinkedHashMultiset<PgTableDefRow> dbListCopy = LinkedHashMultiset.create(dbList);
    csvList.forEach(dbList::remove);
    dbListCopy.forEach(csvList::remove);

    assertListsEqual(dbList, csvList);
  }

  @Test
  public void pgTableDefGenericTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<PgTableDefRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<PgTableDefRow> csvList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SqlUtil.getSql(SQL_PATH_GENERIC))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbList.add(PgTableDefRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH_GENERIC);
    try (CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        PgTableDefRow csvRow = PgTableDefRow.create(line);
        csvList.add(csvRow);
      }
    }

    LinkedHashMultiset<PgTableDefRow> dbListCopy = LinkedHashMultiset.create(dbList);
    csvList.forEach(dbList::remove);
    dbListCopy.forEach(csvList::remove);

    assertListsEqual(dbList, csvList);
  }
}
