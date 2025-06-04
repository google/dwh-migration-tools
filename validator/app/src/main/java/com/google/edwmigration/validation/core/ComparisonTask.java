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
package com.google.edwmigration.validation.core;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.*;
import com.google.edwmigration.validation.model.UserInputContext;
import com.google.edwmigration.validation.sql.ComparisonSqlGenerator;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.jooq.DataType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ComparisonTask {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonTask.class);
  private static final String BQ_RESULTS_SCHEMA_JSON = "results_schema.json";

  private ComparisonTask() {}

  public static BigQuery getBigQuery() {
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);
    try {
      if (credentialsFile != null) {
        return BigQueryOptions.newBuilder()
            .setCredentials(
                GoogleCredentials.fromStream(
                    FileUtils.openInputStream(FileUtils.getFile(credentialsFile))))
            .build()
            .getService();
      } else {
        return BigQueryOptions.getDefaultInstance().getService();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error creating BigQuery client", e);
    }
  }

  public static void createBqResultsTableIfNotExists(UserInputContext context) {
    BigQuery bigQuery = getBigQuery();

    String[] tableId = context.resultTable.name.split("\\.");
    String resultProject = null;
    String resultDataset;
    String resultTable;

    if (tableId.length == 2) {
      resultDataset = tableId[0];
      resultTable = tableId[1];
    } else if (tableId.length == 3) {
      resultProject = tableId[0];
      resultDataset = tableId[1];
      resultTable = tableId[2];
    } else {
      throw new IllegalArgumentException("Invalid BQ result table ID: " + context.resultTable.name);
    }

    TableId fullTableId =
        (resultProject != null)
            ? TableId.of(resultProject, resultDataset, resultTable)
            : TableId.of(resultDataset, resultTable);

    if (bigQuery.getTable(fullTableId) != null) {
      LOG.debug("BQ results table already exists: " + fullTableId);
      return;
    }

    Schema schema = loadSchemaFromJson();
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.of(fullTableId, tableDefinition);

    try {
      bigQuery.create(tableInfo);
      LOG.info("Created BQ results table: " + fullTableId);
    } catch (BigQueryException e) {
      throw new RuntimeException("Failed to create BQ results table: " + e.getMessage(), e);
    }
  }

  private static Schema loadSchemaFromJson() {
    URL resource = ComparisonTask.class.getClassLoader().getResource(BQ_RESULTS_SCHEMA_JSON);
    String jsonContent;

    try {
      if (resource != null) {
        jsonContent = new String(Files.readAllBytes(Paths.get(resource.toURI())));
      } else {
        throw new RuntimeException("Error reading JSON BQ schema file: " + BQ_RESULTS_SCHEMA_JSON);
      }

    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Error reading JSON BQ schema file: " + resource);
    }

    JSONArray jsonArray = new JSONArray(jsonContent);
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < jsonArray.length(); i++) {
      JSONObject jsonField = jsonArray.getJSONObject(i);
      String fieldName = jsonField.getString("name");
      String fieldType = jsonField.getString("type");

      StandardSQLTypeName typeName = StandardSQLTypeName.valueOf(fieldType);
      Field field = Field.of(fieldName, typeName);
      fields.add(field);
    }

    return Schema.of(fields);
  }

  public static void executeComparisonQuery(UserInputContext context, String query)
      throws InterruptedException {
    BigQuery bigQuery = getBigQuery();
    String[] tableId = context.resultTable.name.split("\\.");
    String resultProject = (tableId.length == 3) ? tableId[0] : null;
    String resultDataset = tableId[tableId.length - 2];
    String resultTable = tableId[tableId.length - 1];
    TableId destinationTable =
        (resultProject != null)
            ? TableId.of(resultProject, resultDataset, resultTable)
            : TableId.of(resultDataset, resultTable);

    QueryJobConfiguration config =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(destinationTable)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

    Job queryJob = bigQuery.create(JobInfo.of(JobId.of(), config)).waitFor();
    if (queryJob == null || queryJob.getStatus().getError() != null) {
      throw new RuntimeException(
          "BQ query failed: " + (queryJob == null ? "null job" : queryJob.getStatus().getError()));
    }

    LOG.info("BQ query succeeded. Results written to: " + destinationTable);
  }

  public static HashMap<String, DataType<?>> executeColumnMetadataQuery(
      UserInputContext context, ComparisonSqlGenerator generator, String query)
      throws InterruptedException {
    BigQuery bigQuery = getBigQuery();
    Job job =
        bigQuery
            .create(JobInfo.of(JobId.of(), QueryJobConfiguration.newBuilder(query).build()))
            .waitFor();

    if (job == null || job.getStatus().getError() != null) {
      throw new RuntimeException(
          "Column metadata query failed: "
              + (job == null ? "null job" : job.getStatus().getError()));
    }

    TableResult result = job.getQueryResults();
    HashMap<String, DataType<?>> results = new HashMap<>();
    for (FieldValueList row : result.iterateAll()) {
      String word = row.get(0).getStringValue();
      String dataType = row.get(1).getStringValue();
      results.put(word, generator.getSqlDataType(dataType));
    }

    return results;
  }
}
