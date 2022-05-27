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

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.LinkedHashMultiset.create;
import static com.google.common.truth.Truth.assertThat;
import static com.google.edwmigration.dumper.base.TestBase.CSV_PARSER;
import static com.google.edwmigration.dumper.base.TestBase.assertDbCsvDataEqual;
import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getDbColumnNames;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static com.google.edwmigration.dumper.sql.SqlUtil.getSql;
import static java.sql.DriverManager.getConnection;

import com.google.common.collect.LinkedHashMultiset;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;
import com.opencsv.exceptions.CsvException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class SvvTablesTest {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "svv_tables.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "svv_tables.csv";

  @Test
  public void svvTablesTest() throws SQLException, IOException, CsvException {
    LinkedHashMultiset<Map<String, String>> dbMultiset = create();
    LinkedHashMultiset<Map<String, String>> csvMultiset = create();
    LinkedHashMultiset<String> dbColumnHeaders, csvColumnHeaders;

    try (Connection connection = getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
        PreparedStatement preparedStatement = connection.prepareStatement(getSql(SQL_PATH))) {
      ResultSet rs = preparedStatement.executeQuery();

      dbColumnHeaders = create(getDbColumnNames(rs.getMetaData()));

      while (rs.next()) {
        Map<String, String> dbRow = new HashMap<>();
        for (String header : dbColumnHeaders) {
          dbRow.put(header, getStringNotNull(rs, header));
        }
        dbMultiset.add(dbRow);
      }
    }

    try (FileReader fileReader = new FileReader(CSV_FILE_PATH);
        CSVReaderHeaderAware reader =
            new CSVReaderHeaderAwareBuilder(fileReader).withCSVParser(CSV_PARSER).build()) {
      Map<String, String> csvRow;
      while ((csvRow = reader.readMap()) != null) {
        csvMultiset.add(csvRow);
      }
    }

    csvColumnHeaders =
        create(getFirst(csvMultiset.elementSet(), new HashMap<String, String>()).keySet());

    assertThat(dbColumnHeaders).containsExactlyElementsIn(csvColumnHeaders);
    assertDbCsvDataEqual(dbMultiset, csvMultiset);
  }
}
