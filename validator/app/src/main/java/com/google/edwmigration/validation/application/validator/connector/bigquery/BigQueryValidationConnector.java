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
package com.google.edwmigration.validation.application.validator.connector.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.task.AbstractSourceTask;
import com.google.edwmigration.validation.application.validator.task.AbstractTargetTask;
import java.net.URI;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.NotImplementedException;
import org.jooq.SQLDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author nehanene */
public class BigQueryValidationConnector extends BigQueryAbstractConnector {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryValidationConnector.class);

  public BigQueryValidationConnector() {
    super("bigquery");
  }

  public static class BigQueryTargetTask extends AbstractTargetTask {

    public BigQueryTargetTask(Handle handle, URI outputUri, ValidationArguments arguments) {
      super(handle, outputUri, arguments);
    }

    @Override
    public void run() throws Exception {
      BigQueryHandle bqHandle = (BigQueryHandle) getHandle();
      BigQuery bigQuery = bqHandle.getBigQuery();

      BigQueryTargetSqlGenerator generator =
          new BigQueryTargetSqlGenerator(
              SQLDialect.MYSQL,
              getArguments().getTable().getSource(),
              getArguments().getOptConfidenceInterval(),
              getArguments().getColumnMappings());
      String numericColsQuery = generator.getNumericColumnsQuery();

      // executeQuery
      // LOG.debug(String.valueOf(numericCols));
      // String aggregateQuery = generator.getAggregateQuery(numericCols);
      // LOG.debug(aggregateQuery);
      String rowSampleQuery = generator.getRowSampleQuery();
    }
  }

  @Nonnull
  @Override
  public AbstractSourceTask getSourceQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments) {
    throw new NotImplementedException("BigQuery as a source is not implemented.");
  }

  @Nonnull
  @Override
  public AbstractTargetTask getTargetQueryTask(
      Handle handle, URI outputUri, ValidationArguments arguments) {
    return new BigQueryTargetTask(handle, outputUri, arguments);
  }
}
