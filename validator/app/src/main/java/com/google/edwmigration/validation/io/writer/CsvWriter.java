/*
 * Copyright 2022-2025 Google LLC
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
package com.google.edwmigration.validation.io.writer;

import com.google.edwmigration.validation.config.ValidationType;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvWriter implements ResultSetWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CsvWriter.class);
  private final String csvFilePath;

  public static final String CSV_AGGREGATE_SUFFIX = "_agg.csv";
  public static final String CSV_ROW_SUFFIX = "_row.csv";

  public CsvWriter(URI outputUri, String tableName, ValidationType validationType) {
    this.csvFilePath = createCsvFilePath(outputUri, tableName, validationType);
  }

  private String createCsvFilePath(URI outputUri, String tableName, ValidationType validationType) {
    String filename =
        validationType == ValidationType.AGGREGATE
            ? tableName + CSV_AGGREGATE_SUFFIX
            : tableName + CSV_ROW_SUFFIX;
    Path filePath = Paths.get(outputUri).resolve(filename);
    return filePath.toString();
  }

  @Override
  public Void extractData(ResultSet resultSet) throws SQLException, DataAccessException {
    try (FileWriter fileWriter = new FileWriter(csvFilePath);
        CSVPrinter csvPrinter =
            new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(resultSet))) {

      csvPrinter.printRecords(resultSet);

      LOG.debug("CSV file written to: " + csvFilePath);
    } catch (IOException e) {
      throw new DataAccessException("Failed to write CSV to " + csvFilePath, e) {};
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("An error occurred in CSVPrinter", e) {};
    }
    return null;
  }
}
