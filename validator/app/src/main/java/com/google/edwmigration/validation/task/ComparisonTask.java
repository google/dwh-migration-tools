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
package com.google.edwmigration.validation.task;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.edwmigration.validation.NameManager;
import com.google.edwmigration.validation.NameManager.ValidationType;
import com.google.edwmigration.validation.ValidationArguments;
import com.google.edwmigration.validation.ValidationColumnMapping;
import com.google.edwmigration.validation.sql.ComparisonSqlGenerator;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.annotation.CheckForNull;
import org.apache.commons.io.FileUtils;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class ComparisonTask {

  private static final Logger LOG = LoggerFactory.getLogger(ComparisonTask.class);
  private String resultProject = null;
  private final String resultDataset;
  private final String resultTable;

  private final ValidationArguments args;
  private final NameManager nameManager;
  private final String BQ_RESULTS_SCHEMA_JSON = "results_schema.json";

  public ComparisonTask(ValidationArguments args, NameManager nameManager) {
    String[] tableId = args.getBqResultsTable().split("\\.");
    if (tableId.length == 2) {
      this.resultDataset = tableId[0];
      this.resultTable = tableId[1];
    } else if (tableId.length == 3) {
      this.resultProject = tableId[0];
      this.resultDataset = tableId[1];
      this.resultTable = tableId[2];
    } else {
      throw new IllegalArgumentException(
          "Invalid BQ result table ID. Please provide `project.dataset.table`: "
              + args.getBqResultsTable());
    }

    this.args = args;
    this.nameManager = nameManager;
  }

  public ValidationArguments getArguments() {
    return args;
  }

  @CheckForNull
  public String getResultProject() {
    return resultProject;
  }

  public NameManager getNameManager() {
    return nameManager;
  }

  public String getResultDataset() {
    return resultDataset;
  }

  public String getResultTable() {
    return resultTable;
  }

  public static BigQuery getBigQuery() {
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);
    BigQuery bigQuery;
    try {
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
    } catch (IOException e) {
      throw new RuntimeException("Error creating BigQuery client", e);
    }
    return bigQuery;
  }

  public void createBqResultsTableIfNotExists(String datasetName, String tableName) {
    BigQuery bigQuery = getBigQuery();

    TableId tableId;
    if (getResultProject() != null) {
      tableId = TableId.of(getResultProject(), datasetName, tableName);
    } else {
      tableId = TableId.of(datasetName, tableName);
    }

    Table table = bigQuery.getTable(tableId);
    if (table != null && table.exists()) {
      LOG.debug(
          "BQ results table already exists, not creating: "
              + tableId.getDataset()
              + "."
              + tableId.getTable());
      return;
    }

    Schema schema = loadSchemaFromJson();

    TableDefinition tableDefinition =
        StandardTableDefinition.newBuilder().setSchema(schema).build();
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    try {
      Table newTable = bigQuery.create(tableInfo);
      LOG.debug(
          "BQ results table created successfully: "
              + newTable.getTableId().getDataset()
              + "."
              + newTable.getTableId().getTable());
    } catch (BigQueryException e) {
      LOG.error("Error creating BQ results table: " + e.getMessage());
      throw new RuntimeException("Error creating BQ results table: " + tableId);
    }
  }

  protected Schema loadSchemaFromJson() {
    URL resource = this.getClass().getClassLoader().getResource(BQ_RESULTS_SCHEMA_JSON);
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

  public HashMap<String, DataType<?>> executeColumnMetadataQuery(
      ComparisonSqlGenerator generator, String query) throws Exception {
    BigQuery bigQuery = getBigQuery();

    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

    JobId jobId = JobId.of();
    Job queryJob = bigQuery.create(Job.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException(
          "Error executing BQ metadata query. Job no longer exists:" + jobId.toString());
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(
          "Error executing BQ metadata query." + queryJob.getStatus().getError().toString());
    }

    TableResult result = queryJob.getQueryResults();
    HashMap<String, DataType<?>> results = new HashMap<>();

    for (FieldValueList row : result.iterateAll()) {
      String word = row.get(0).getStringValue();
      String dataType = row.get(1).getStringValue();
      DataType<?> sqlDataType = generator.getSqlDataType(dataType);
      results.put(word, sqlDataType);
    }
    LOG.debug(String.valueOf(results));

    return results;
  }

  public void executeComparisonQuery(String query) throws Exception {
    BigQuery bigQuery = getBigQuery();

    TableId destinationTable = TableId.of(getResultDataset(), getResultTable());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(destinationTable)
            .setWriteDisposition(WriteDisposition.WRITE_APPEND)
            .build();

    JobId jobId = JobId.of();
    Job queryJob = bigQuery.create(Job.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException(
          "Error executing BQ comparison query. Job no longer exists:" + jobId.toString());
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(
          "Error executing BQ comparison query." + queryJob.getStatus().getError().toString());
    } else {
      LOG.info(
          String.format(
              "Comparison query results successfully written to BQ results table with ID: %s and job ID: %s",
              getArguments().getBqResultsTable(), jobId.toString()));
    }
  }

  public void run() throws Exception {
    createBqResultsTableIfNotExists(getResultDataset(), getResultTable());
    ComparisonSqlGenerator generator =
        new ComparisonSqlGenerator(
            SQLDialect.MYSQL, getArguments().getTableMapping(), getNameManager());
    String aggCompareQuery = generator.getAggregateCompareQuery();
    executeComparisonQuery(aggCompareQuery);

    String sourceColMetadataQuery =
        generator.getColumnMetadataQuery(getNameManager().getBqSourceTableName(ValidationType.ROW));
    HashMap<String, DataType<?>> rowSourceColumns =
        executeColumnMetadataQuery(generator, sourceColMetadataQuery);

    String targetColMetadataQuery =
        generator.getColumnMetadataQuery(getNameManager().getBqTargetTableName(ValidationType.ROW));
    HashMap<String, DataType<?>> rowTargetColumns =
        executeColumnMetadataQuery(generator, targetColMetadataQuery);

    ValidationColumnMapping validationColumnMapping =
        new ValidationColumnMapping(
            getArguments().getColumnMappings(),
            rowSourceColumns,
            rowTargetColumns,
            getArguments().getPrimaryKeys());

    String rowCompareQuery = generator.getRowCompareQuery(validationColumnMapping);
    executeComparisonQuery(rowCompareQuery);
  }
}
