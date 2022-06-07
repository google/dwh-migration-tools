package com.google.integration;

import static com.google.edwmigration.dumper.base.TestBase.CSV_PARSER;
import static com.google.edwmigration.dumper.base.TestBase.assertMultisetsEqual;
import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;
import static com.google.edwmigration.dumper.sql.SqlUtil.getSql;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.pojo.PgTypesRow;
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
import org.testng.annotations.Test;

public class PgTypesTest {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "pg_types.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "types.csv";

  @Test
  public void pgTypesTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<PgTypesRow> dbMultiset = LinkedHashMultiset.create();
    LinkedHashMultiset<PgTypesRow> csvMultiset = LinkedHashMultiset.create();

    try (Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB)) {
      try (PreparedStatement preparedStatement = connection.prepareStatement(getSql(SQL_PATH))) {
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()) {
          dbMultiset.add(PgTypesRow.create(rs));
        }
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH);
    try (CSVReader reader =
        new CSVReaderBuilder(fileReader).withSkipLines(1).withCSVParser(CSV_PARSER).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        PgTypesRow csvRow = PgTypesRow.create(line);
        csvMultiset.add(csvRow);
      }
    }

    assertMultisetsEqual(dbMultiset, csvMultiset);
  }
}
