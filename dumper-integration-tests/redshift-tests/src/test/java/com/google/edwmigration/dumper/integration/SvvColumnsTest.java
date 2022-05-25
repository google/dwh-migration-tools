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

import static com.google.edwmigration.dumper.base.TestBase.CSV_PARSER;
import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getDbColumnNames;
import static com.google.edwmigration.dumper.jdbc.JdbcUtil.getStringNotNull;
import static com.google.edwmigration.dumper.sql.SqlUtil.getSql;
import static org.testng.Assert.assertEquals;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.truth.Truth;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;
import com.opencsv.exceptions.CsvException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.testng.annotations.Test;

public class SvvColumnsTest {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "svv_columns.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "svv_columns.csv";

  @Test
  public void svvColumnsTest() throws SQLException, IOException, CsvException {
    LinkedHashMultiset<String> dbMultiset = LinkedHashMultiset.create();
    LinkedHashMultiset<String> csvMultiset = LinkedHashMultiset.create();
    int dbColumnCount, csvColumnCount;
    LinkedHashMultiset<String> dbColumnHeaders, csvColumnHeaders;

    try (Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
        PreparedStatement preparedStatement = connection.prepareStatement(getSql(SQL_PATH))) {
      ResultSet rs = preparedStatement.executeQuery();

      dbColumnCount = rs.getMetaData().getColumnCount();
      dbColumnHeaders = LinkedHashMultiset.create(getDbColumnNames(rs.getMetaData()));

      while (rs.next()) {
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= dbColumnCount; i++) {
          sb.append(getStringNotNull(rs, i));
          if (i < dbColumnCount) {
            sb.append(",");
          }
        }
        dbMultiset.add(sb.toString());
      }
    }

    try (FileReader fileReader = new FileReader(CSV_FILE_PATH);
        CSVReaderHeaderAware reader =
            new CSVReaderHeaderAwareBuilder(fileReader).withCSVParser(CSV_PARSER).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        csvMultiset.add(Joiner.on(",").join(line));
      }
    }

    try (FileReader fileReader = new FileReader(CSV_FILE_PATH);
        CSVReaderHeaderAware reader =
            new CSVReaderHeaderAwareBuilder(fileReader).withCSVParser(CSV_PARSER).build()) {
      Map<String, String> lineMap = reader.readMap();
      csvColumnCount = lineMap.keySet().size();
      csvColumnHeaders = LinkedHashMultiset.create(lineMap.keySet());
    }

    assertEquals(dbColumnCount, csvColumnCount, "Table header count is different");

    // dbColumnHeaders.remove(Iterables.getFirst(dbColumnHeaders.elementSet(), null)); // remove the first header from dbColumnHeaders

    dbMultiset.remove(Iterables.getFirst(dbMultiset.elementSet(), null));       // remove the first row from deb extraction
    // csvMultiset.remove(Iterables.getLast(csvMultiset.elementSet(), null));   // remove the last row from deb extraction

    Truth.assertThat(dbColumnCount).isEqualTo(csvColumnCount);
    Truth.assertThat(dbColumnHeaders).containsExactlyElementsIn(csvColumnHeaders);

    Truth.assertThat(dbMultiset).containsExactlyElementsIn(csvMultiset);
    // assertDbCsvDataEqual(dbMultiset, csvMultiset);
  }
}
