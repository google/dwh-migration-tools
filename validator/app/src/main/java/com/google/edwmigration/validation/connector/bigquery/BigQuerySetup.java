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
package com.google.edwmigration.validation.connector.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.edwmigration.validation.util.BigQuerySchemaConverter;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySetup {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySetup.class);

  private BigQuerySetup() {}

  public static void createBqExternalTable(
      ResultSetMetaData metadata,
      String bqStagingDataset,
      String sourceGcsUri,
      String externalTableName,
      String projectId)
      throws SQLException {
    Schema schema = BigQuerySchemaConverter.from(metadata);
    BigQuery bigquery = BigQueryHandle.create(projectId);
    CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
    TableId tableId = TableId.of(bqStagingDataset, externalTableName);

    LOG.debug(
        String.format("Creating external table from %s to BQ table ID %s", sourceGcsUri, tableId));

    ExternalTableDefinition externalTable =
        ExternalTableDefinition.newBuilder(sourceGcsUri, csvOptions).setSchema(schema).build();

    try {
      boolean deleted = bigquery.delete(tableId);
      if (deleted) {
        LOG.debug("Deleted existing external table with ID: " + tableId);
      }
      bigquery.create(TableInfo.of(tableId, externalTable));
    } catch (BigQueryException e) {
      throw new RuntimeException("Error creating BigQuery external table from GCS file", e);
    }
  }
}
