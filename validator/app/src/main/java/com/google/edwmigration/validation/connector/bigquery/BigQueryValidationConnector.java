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

import com.google.edwmigration.validation.config.ValidationConfig;
import com.google.edwmigration.validation.core.BqNameFormatter;
import com.google.edwmigration.validation.handle.Handle;
import com.google.edwmigration.validation.task.AbstractSourceTask;
import com.google.edwmigration.validation.task.AbstractTargetTask;
import java.net.URI;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class BigQueryValidationConnector extends BigQueryAbstractConnector {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryValidationConnector.class);

  public BigQueryValidationConnector() {
    super("bigquery");
  }

  public static class BigQueryTargetTask extends AbstractTargetTask {

    public BigQueryTargetTask(
        Handle handle, BqNameFormatter bqNameFormatter, ValidationConfig arguments) {
      super(handle, bqNameFormatter, arguments);
    }

    @Override
    public void run() throws Exception {
      //   BigQuerySqlGenerator generator =
      //       new BigQuerySqlGenerator(
      //           SQLDialect.MYSQL,
      //           getArguments().tableMapping,
      //           .99,
      //           getArguments().columnMapping,
      //           TableType.TARGET,
      //           ImmutableMap.of("dingo", "id")
      //         );
      //   String numericColsQuery = generator.getNumericColumnsQuery();
      //   LOG.debug(numericColsQuery);
      //   HashMap<String, DataType<? extends Number>> numericCols =
      //       executeNumericColsQuery(generator, numericColsQuery);

      //   String aggregateQuery = generator.getAggregateQuery(numericCols);
      //   String aggTargetTable =
      // getBqNameFormatter().getBqTargetTableName(ValidationType.AGGREGATE);
      //   extractQueryResultsToTable(aggregateQuery, aggTargetTable);

      //   String rowSampleQuery = generator.getRowSampleQuery();
      //   String rowTargetTable = getBqNameFormatter().getBqTargetTableName(ValidationType.ROW);
      //   extractQueryResultsToTable(rowSampleQuery, rowTargetTable);
    }
  }

  @Nonnull
  @Override
  public AbstractSourceTask getSourceQueryTask(
      Handle handle, URI outputUri, ValidationConfig arguments) {
    throw new NotImplementedException("BigQuery as a source is not implemented.");
  }

  @Nonnull
  @Override
  public AbstractTargetTask getTargetQueryTask(
      Handle handle, BqNameFormatter bqNameFormatter, ValidationConfig arguments) {
    return new BigQueryTargetTask(handle, bqNameFormatter, arguments);
  }
}
