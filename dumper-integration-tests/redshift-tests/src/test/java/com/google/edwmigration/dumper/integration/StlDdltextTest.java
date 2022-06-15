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
import static com.google.edwmigration.dumper.base.TestConstants.EXTRACTION_END_TIME;
import static com.google.edwmigration.dumper.base.TestConstants.EXTRACTION_START_TIME;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.base.TestBase;
import com.google.edwmigration.dumper.pojo.StlDdltextRow;
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

public class StlDdltextTest extends TestBase {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "stl_ddltext.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "ddltext_all.csv";

  private static Connection connection;

  @BeforeClass
  public void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void stlDdltextTest() throws IOException, SQLException, CsvValidationException {
    LinkedHashMultiset<StlDdltextRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<StlDdltextRow> csvList = LinkedHashMultiset.create();

    String query = String.format(SqlUtil.getSql(SQL_PATH), EXTRACTION_START_TIME, EXTRACTION_END_TIME);

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(query)) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbList.add(StlDdltextRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH);
    try (CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        StlDdltextRow csvRow = StlDdltextRow.create(line);
        csvList.add(csvRow);
      }
    }

    assertListsEqual(dbList, csvList);
  }


}
