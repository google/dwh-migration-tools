package com.google.edwmigration.dumper.integration;

import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.base.TestBase;
import com.google.edwmigration.dumper.pojo.PgDatabaseInfoRow;
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

public class PgDatabaseInfoTest extends TestBase {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "pg_database_info.sql";
  private static final String CSV_FILE_PATH = EXPORTED_FILES_BASE_PATH + "database.csv";

  private static Connection connection;

  @BeforeClass
  public void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void pgDatabaseInfoTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<PgDatabaseInfoRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<PgDatabaseInfoRow> csvList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SqlUtil.getSql(SQL_PATH))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbList.add(PgDatabaseInfoRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH);
    try (CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        PgDatabaseInfoRow csvRow = PgDatabaseInfoRow.create(line);
        csvList.add(csvRow);
      }
    }

    assertListsEqual(dbList, csvList);
  }

}
