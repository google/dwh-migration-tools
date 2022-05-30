package com.google.edwmigration.dumper.integration;

import static com.google.edwmigration.dumper.base.TestConstants.EXPORTED_FILES_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.PASSWORD_DB;
import static com.google.edwmigration.dumper.base.TestConstants.SQL_REQUESTS_BASE_PATH;
import static com.google.edwmigration.dumper.base.TestConstants.URL_DB;
import static com.google.edwmigration.dumper.base.TestConstants.USERNAME_DB;

import com.google.common.collect.LinkedHashMultiset;
import com.google.edwmigration.dumper.base.TestBase;
import com.google.edwmigration.dumper.pojo.SvvExternalPartitionsRow;
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

public class SvvExternalPartitionsTest extends TestBase {

  private static final String SQL_PATH = SQL_REQUESTS_BASE_PATH + "svv_external_partitions.sql";
  private static final String CSV_FILE_PATH =
      EXPORTED_FILES_BASE_PATH + "svv_external_partitions.csv";

  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void svvExternalPartitionsTest() throws SQLException, IOException, CsvValidationException {
    LinkedHashMultiset<SvvExternalPartitionsRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<SvvExternalPartitionsRow> csvList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(SqlUtil.getSql(SQL_PATH))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        dbList.add(SvvExternalPartitionsRow.create(rs));
      }
    }

    FileReader fileReader = new FileReader(CSV_FILE_PATH);
    try (CSVReader reader = new CSVReaderBuilder(fileReader).withSkipLines(1).build()) {
      String[] line;
      while ((line = reader.readNext()) != null) {
        SvvExternalPartitionsRow csvRow = SvvExternalPartitionsRow.create(line);
        csvList.add(csvRow);
      }
    }

    LinkedHashMultiset<SvvExternalPartitionsRow> dbListCopy = LinkedHashMultiset.create(dbList);
    csvList.forEach(dbList::remove);
    dbListCopy.forEach(csvList::remove);

    assertListsEqual(dbList, csvList);
  }
}
