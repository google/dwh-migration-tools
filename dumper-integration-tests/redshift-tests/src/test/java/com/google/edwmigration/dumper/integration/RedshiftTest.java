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
import com.opencsv.exceptions.CsvValidationException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RedshiftTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftTest.class);

  @DataProvider(name = "redshift-tables")
  public Object[][] dataProviderMethod() {
    return new Object[][] {
      {"svv_columns.sql", "svv_columns.csv", "ssv_columns"},
      {"svv_tables.sql", "svv_tables.csv", "svv_tables"},
      {"svv_table_info.sql", "svv_table_info.csv", "svv_table_info"},
      {"svv_external_schemas.sql", "svv_external_schemas.csv", "svv_external_schemas"},
      {"svv_external_databases.sql", "svv_external_databases.csv", "svv_external_databases"},
      {"svv_external_tables.sql", "svv_external_tables.csv", "svv_external_tables"},
      {"svv_external_columns.sql", "svv_external_columns.csv", "svv_external_columns"},
      {"svv_external_partitions.sql", "svv_external_partitions.csv", "svv_external_partitions"},
      {"pg_library.sql", "pg_library.csv", "pq_library"},
      {"pg_database.sql", "pg_database.csv", "pg_database"},
      {"pg_namespace.sql", "pg_namespace.csv", "pg_namespace"},
      {"pg_operator.sql", "pg_operator.csv", "pg_operator"},
      {"pg_tables.sql", "pg_tables.csv", "pg_tables"},
      {"pg_table_def_generic.sql", "pg_table_def_generic.csv", "pg_table_def_generic"},
      {"pg_table_def_private.sql", "pg_table_def_private.csv", "pg_table_def_private"},
      {"pg_views_generic.sql", "pg_views_generic.csv", "pg_views_generic"},
      {"pg_views_private.sql", "pg_views_private.csv", "pg_views_private"},
      {"pg_user.sql", "pg_user.csv", "pg_user"},
      {"pg_database_info.sql", "database.csv", "pg_database_info"},
      {"pg_proc.sql", "aggregates.csv", "pg_proc"},
      {"pg_type.sql", "types.csv", "pg_type"},
      {"pg_cast.sql", "casts.csv", "pg_cast"}
    };
  }

  @Test(dataProvider = "redshift-tables")
  public void redshiftTest(String sql, String csvFile, String tableName)
      throws SQLException, IOException, CsvValidationException {
    LOGGER.info("Running test for {} table", tableName);
    String sqlPath = SQL_REQUESTS_BASE_PATH + sql;
    String csvPath = EXPORTED_FILES_BASE_PATH + csvFile;
    LinkedHashMultiset<Map<String, String>> dbMultiset = create();
    LinkedHashMultiset<Map<String, String>> csvMultiset = create();
    LinkedHashMultiset<String> dbColumnHeaders, csvColumnHeaders;

    try (Connection connection = getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
        PreparedStatement preparedStatement = connection.prepareStatement(getSql(sqlPath))) {
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

    try (FileReader fileReader = new FileReader(csvPath);
        CSVReaderHeaderAware reader =
            new CSVReaderHeaderAwareBuilder(fileReader).withCSVParser(CSV_PARSER).build()) {
      Map<String, String> csvRow;
      while ((csvRow = reader.readMap()) != null) {
        replaceBooleanValues(csvRow);
        replaceInfinityValues(csvRow);
        csvMultiset.add(csvRow);
      }
    }

    csvColumnHeaders =
        create(getFirst(csvMultiset.elementSet(), new HashMap<String, String>()).keySet());

    assertThat(dbColumnHeaders).containsExactlyElementsIn(csvColumnHeaders);
    assertDbCsvDataEqual(dbMultiset, csvMultiset);
  }

  // db returns infinity but in csv is max date 292278994-08-17 00:00:00.0
  private void replaceInfinityValues(Map<String, String> csvRow) {
    for (String key : csvRow.keySet()) {
      if (csvRow.get(key).equals("292278994-08-17 00:00:00.0")) {
        csvRow.replace(key, "infinity");
      }
    }
  }

  // Database returns for booleans t, f but in csv are full names
  private void replaceBooleanValues(Map<String, String> csvRow) {
    for (String key : csvRow.keySet()) {
      if (csvRow.get(key).equals("true")) {
        csvRow.replace(key, "t");
        continue;
      }
      if (csvRow.get(key).equals("false")) {
        csvRow.replace(key, "f");
      }
    }
  }
}
