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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQuerySetupTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSourceTask.class);
  private final ResultSetMetaData metadata;
  private final String bqStagingDataset;
  private final String sourceGcsUri;
  private final String externalTableName;

  public BigQuerySetupTask(
      ResultSetMetaData metadata,
      String bqStagingDataset,
      String sourceGcsUri,
      String externalTableName) {
    this.metadata = metadata;
    this.bqStagingDataset = bqStagingDataset;
    this.sourceGcsUri = sourceGcsUri;
    this.externalTableName = externalTableName;
  }

  public void createBqExternalTable() throws SQLException {
    Schema schema = createBigQuerySchema(metadata);
    try {
      BigQuery bigquery = getBigQuery();

      CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();

      TableId tableId = TableId.of(bqStagingDataset, externalTableName);
      LOG.debug(
          String.format(
              "Creating external table from %s to BQ table ID %s", sourceGcsUri, tableId));
      ExternalTableDefinition externalTable =
          ExternalTableDefinition.newBuilder(sourceGcsUri, csvOptions).setSchema(schema).build();
      bigquery.create(TableInfo.of(tableId, externalTable));

    } catch (BigQueryException e) {
      throw new RuntimeException("Error creating BigQuery external table from GCS file", e);
    } catch (IOException e) {
      throw new RuntimeException("Error creating BigQuery client", e);
    }
  }

  private static BigQuery getBigQuery() throws IOException {
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);
    BigQuery bigQuery;
    if (credentialsFile != null) {
      bigQuery =
          BigQueryOptions.newBuilder()
              .setCredentials(
                  GoogleCredentials.fromStream(
                      FileUtils.openInputStream(FileUtils.getFile(credentialsFile))))
              .build()
              .getService();
    } else {
      bigQuery = BigQueryOptions.getDefaultInstance().getService();
    }
    return bigQuery;
  }

  private Schema createBigQuerySchema(ResultSetMetaData metaData) throws SQLException {

    List<Field> fieldList = new ArrayList<>();
    int columnCount = metaData.getColumnCount();

    for (int i = 1; i <= columnCount; i++) {
      String columnName = metaData.getColumnName(i);
      int columnType = metaData.getColumnType(i);
      StandardSQLTypeName bqType = getBigQueryType(columnType);

      Field.Builder fieldBuilder = Field.newBuilder(columnName, bqType);
      fieldList.add(fieldBuilder.build());
    }

    return Schema.of(fieldList);
  }

  private static StandardSQLTypeName getBigQueryType(int columnType) {
    switch (columnType) {
      case Types.BIGINT:
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT:
        return StandardSQLTypeName.INT64;
      case Types.BOOLEAN:
        return StandardSQLTypeName.BOOL;
      case Types.DATE:
        return StandardSQLTypeName.DATE;
      case Types.DECIMAL:
      case Types.NUMERIC:
        return StandardSQLTypeName.NUMERIC;
      case Types.DOUBLE:
      case Types.FLOAT:
        return StandardSQLTypeName.FLOAT64;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CLOB:
        return StandardSQLTypeName.STRING;
      case Types.TIMESTAMP:
        return StandardSQLTypeName.TIMESTAMP;
      case Types.TIME:
        return StandardSQLTypeName.TIME;
      default:
        // Handle unknown or unsupported types
        LOG.error("Warning: Unhandled SQL type: " + columnType + ". Defaulting to STRING.");
        return StandardSQLTypeName.STRING;
    }
  }
}
