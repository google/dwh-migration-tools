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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.*;
import com.google.common.base.Preconditions;
import com.google.edwmigration.validation.connector.api.Handle;
import com.google.edwmigration.validation.connector.common.AbstractConnector;
import com.google.edwmigration.validation.connector.common.AbstractHandle;
import com.google.edwmigration.validation.connector.common.AbstractSourceTask;
import com.google.edwmigration.validation.connector.common.AbstractTargetTask;
import com.google.edwmigration.validation.io.writer.ResultSetWriterFactory;
import com.google.edwmigration.validation.model.ExecutionState;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BigQueryValidationConnector extends AbstractConnector {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryValidationConnector.class);

  // TODO this is another runnable turducken
  public static class BigQueryHandle extends AbstractHandle {

    private final BigQuery bigQuery;

    public BigQueryHandle(@Nonnull BigQuery bigQuery) {
      LOG.debug("creating inner BigQueryHandle");
      this.bigQuery = Preconditions.checkNotNull(bigQuery, "BigQuery was null.");
    }

    @Nonnull
    public BigQuery getBigQuery() {
      return bigQuery;
    }
  }

  public BigQueryValidationConnector() {
    super("bigquery");
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ExecutionState state) throws Exception {
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);

    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
    if (credentialsFile != null) {
      builder.setCredentials(
          GoogleCredentials.fromStream(
              FileUtils.openInputStream(FileUtils.getFile(credentialsFile))));
    }

    BigQuery bigQuery =
        builder.setProjectId(state.context.bqTargetTable.projectId).build().getService();

    return new BigQueryHandle(bigQuery);
  }

  @Nonnull
  @Override
  public AbstractSourceTask getSourceQueryTask(
      ExecutionState state, ResultSetWriterFactory writerFactory) {
    throw new NotImplementedException("BigQuery as a source is not implemented.");
  }

  @Nonnull
  @Override
  public AbstractTargetTask getTargetQueryTask(ExecutionState state) {
    throw new NotImplementedException("What is this even for?");
  }

  // @Override
  //   public void run() throws Exception {
  //     BigQuerySqlGenerator generator =
  //         new BigQuerySqlGenerator(
  //             SQLDialect.MYSQL,
  //             getArguments().tableMapping,
  //             .99,
  //             getArguments().columnMapping,
  //             TableType.TARGET,
  //             ImmutableMap.of("dingo", "id")
  //           );

  //     String numericColsQuery = generator.getNumericColumnsQuery();
  //     LOG.debug(numericColsQuery);
  //     HashMap<String, DataType<? extends Number>> numericCols =
  //         executeNumericColsQuery(generator, numericColsQuery);

  //     String aggregateQuery = generator.getAggregateQuery(numericCols);
  //     String aggTargetTable =
  //   getBqNameFormatter().getBqTargetTableName(ValidationType.AGGREGATE);
  //     extractQueryResultsToTable(aggregateQuery, aggTargetTable);

  //     String rowSampleQuery = generator.getRowSampleQuery();
  //     String rowTargetTable = getBqNameFormatter().getBqTargetTableName(ValidationType.ROW);
  //     extractQueryResultsToTable(rowSampleQuery, rowTargetTable);
  //   }
}
