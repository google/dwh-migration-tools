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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.base.Preconditions;
import com.google.edwmigration.validation.application.validator.NameManager;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.connector.bigquery.BigQueryAbstractConnector.BigQueryHandle;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.sql.SqlGenerator;
import java.util.HashMap;
import org.jooq.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public abstract class AbstractTargetTask {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTargetTask.class);

  private final Handle handle;
  private final NameManager nameManager;

  private final ValidationArguments arguments;

  public AbstractTargetTask(Handle handle, NameManager nameManager, ValidationArguments arguments) {
    Preconditions.checkNotNull(handle, "Handle is null.");
    this.handle = handle;
    this.nameManager = nameManager;
    this.arguments = arguments;
  }

  public Handle getHandle() {
    return handle;
  }

  public ValidationArguments getArguments() {
    return arguments;
  }

  public NameManager getNameManager() {
    return nameManager;
  }

  public HashMap<String, DataType<? extends Number>> executeNumericColsQuery(
      SqlGenerator generator, String query) throws Exception {
    BigQueryHandle bqHandle = (BigQueryHandle) getHandle();
    BigQuery bigQuery = bqHandle.getBigQuery();

    // Configure the query
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of();
    Job queryJob = bigQuery.create(Job.newBuilder(queryConfig).build());

    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException(
          "Error executing BQ metadata query. Job no longer exists:" + jobId.toString());
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(
          "Error executing BQ metadata query." + queryJob.getStatus().getError().toString());
    }

    TableResult result = queryJob.getQueryResults();
    HashMap<String, DataType<? extends Number>> results = new HashMap<>();

    // Print the results
    for (FieldValueList row : result.iterateAll()) {
      String word = row.get(0).getStringValue();
      String dataType = row.get(1).getStringValue();
      DataType<? extends Number> sqlDataType = generator.getSqlDataType(dataType, null, null);
      results.put(word, sqlDataType);
    }

    return results;
  }

  public void extractQueryResults(String query, String targetTableId) throws Exception {
    BigQueryHandle bqHandle = (BigQueryHandle) getHandle();
    BigQuery bigQuery = bqHandle.getBigQuery();
    String dataset = getArguments().getBqStagingDataset();

    TableId destinationTable = TableId.of(dataset, targetTableId);

    Table existingTable = bigQuery.getTable(destinationTable);
    if (existingTable != null && existingTable.exists()) {
      LOG.warn("Destination table exists and will be overwritten: " + destinationTable);
    } else {
      LOG.warn("Destination table does not exist and will be created: " + destinationTable);
    }

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDestinationTable(destinationTable)
            .setAllowLargeResults(true)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of();
    Job queryJob = bigQuery.create(Job.newBuilder(queryConfig).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException(
          "Error executing BQ query. Job no longer exists:" + jobId.toString());
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(
          "Error executing BQ query." + queryJob.getStatus().getError().toString());
    } else {
      LOG.debug("Query results successfully written to BQ table ID: " + destinationTable);
    }
  }

  public abstract void run() throws Exception;

  public String describeTargetData() {
    return "from" + getClass().getSimpleName();
  }

  public String toString() {
    return format("Querying data %s", describeTargetData());
  }
}
