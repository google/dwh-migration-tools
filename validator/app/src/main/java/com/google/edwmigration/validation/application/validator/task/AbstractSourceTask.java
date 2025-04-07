/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.validation.application.validator.task;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public abstract class AbstractSourceTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSourceTask.class);

  public static final String CSV_AGGREGATE_SUFFIX = "_agg.csv";
  public static final String CSV_ROW_SUFFIX = "_row.csv";

  private final Handle handle;
  private final URI outputUri;
  private final ValidationArguments arguments;

  private ResultSetMetaData aggregateQueryMetadata;
  private ResultSetMetaData rowQueryMetadata;

  public AbstractSourceTask(Handle handle, URI outputUri, ValidationArguments arguments) {
    Preconditions.checkNotNull(handle, "Handle is null.");
    this.handle = handle;
    this.outputUri = outputUri;
    this.arguments = arguments;
  }

  public Handle getHandle() {
    return handle;
  }

  public ValidationArguments getArguments() {
    return arguments;
  }

  public URI getOutputUri() {
    return outputUri;
  }

  public void setAggregateQueryMetadata(ResultSetMetaData metadata) {
    this.aggregateQueryMetadata = metadata;
  }

  public ResultSetMetaData getAggregateQueryMetadata() {
    return this.aggregateQueryMetadata;
  }

  public void setRowQueryMetadata(ResultSetMetaData metadata) {
    this.rowQueryMetadata = metadata;
  }

  public ResultSetMetaData getRowQueryMetadata() {
    return this.rowQueryMetadata;
  }

  public abstract void run() throws Exception;

  public String describeSourceData() {
    return "from" + getClass().getSimpleName();
  }

  public String toString() {
    return format("Write to %s %s", outputUri, describeSourceData());
  }

  interface ResultSetExtractor<T> {
    T extractData(ResultSet rs) throws SQLException, IOException;
  }

  static class CsvResultSetExtractor implements ResultSetExtractor<Void> {
    private final String csvFilePath;

    public CsvResultSetExtractor(String csvFilePath) {
      this.csvFilePath = csvFilePath;
    }

    @Override
    public Void extractData(ResultSet resultSet) throws SQLException, IOException {
      try (FileWriter csvWriter = new FileWriter(csvFilePath)) {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        // Write header row
        for (int i = 1; i <= columnCount; i++) {
          csvWriter.append(metaData.getColumnName(i));
          if (i < columnCount) {
            csvWriter.append(",");
          }
        }
        csvWriter.append("\n");

        // Write data rows
        while (resultSet.next()) {
          for (int i = 1; i <= columnCount; i++) {
            csvWriter.append(resultSet.getString(i));
            if (i < columnCount) {
              csvWriter.append(",");
            }
          }
          csvWriter.append("\n");
        }

        LOG.debug("CSV file written to: " + csvFilePath);
      }

      return null;
    }
  }
}
