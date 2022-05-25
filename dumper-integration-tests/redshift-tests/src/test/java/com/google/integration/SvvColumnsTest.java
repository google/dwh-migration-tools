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
package com.google.integration;

import static com.google.edwmigration.dumper.base.TestBase.CSV_PARSER;
import static com.google.edwmigration.dumper.base.TestBase.assertMultisetsEqual;
import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.pojo.SvvColumnsRow;
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

public class SvvColumnsTest {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "svv_columns.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "svv_columns.csv";
  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void svvColumnsTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<SvvColumnsRow> dbMultiset = LinkedHashMultiset.create();
    LinkedHashMultiset<SvvColumnsRow> csvMultiset = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SqlUtil.getSql(SQL_PATH))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbMultiset.add(SvvColumnsRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH);
    try (CSVReader reader =
        new CSVReaderBuilder(fileReader).withSkipLines(1).withCSVParser(CSV_PARSER).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        SvvColumnsRow csvRow = SvvColumnsRow.create(line);
        csvMultiset.add(csvRow);
      }
    }

    assertMultisetsEqual(dbMultiset, csvMultiset);
  }
}
