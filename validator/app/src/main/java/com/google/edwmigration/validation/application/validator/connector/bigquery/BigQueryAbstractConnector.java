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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.*;
import com.google.common.base.Preconditions;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import com.google.edwmigration.validation.application.validator.ValidationConnection;
import com.google.edwmigration.validation.application.validator.connector.AbstractConnector;
import com.google.edwmigration.validation.application.validator.handle.AbstractHandle;
import com.google.edwmigration.validation.application.validator.handle.Handle;
import com.google.edwmigration.validation.application.validator.task.AbstractTask;
import java.net.URI;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BigQueryAbstractConnector extends AbstractConnector {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryAbstractConnector.class);

  public static class BigQueryHandle extends AbstractHandle {

    private final BigQuery bigQuery;

    public BigQueryHandle(@Nonnull BigQuery bigQuery) {
      this.bigQuery = Preconditions.checkNotNull(bigQuery, "BigQuery was null.");
    }

    @Nonnull
    public BigQuery getBigQuery() {
      return bigQuery;
    }
  }

  public BigQueryAbstractConnector(@Nonnull String name) {
    super(name);
  }

  public abstract class BigQueryAbstractTask extends AbstractTask {

    public BigQueryAbstractTask(Handle handle, URI outputUri, ValidationArguments arguments) {
      super(handle, outputUri, arguments);
    }

    public void executeQuery(String query) throws Exception {
      BigQueryHandle bqHandle = (BigQueryHandle) getHandle();
      BigQuery bigQuery = bqHandle.getBigQuery();

      // Configure the query
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      // Create a job ID so that we can safely retry.
      JobId jobId = JobId.of();
      Job queryJob = bigQuery.create(Job.newBuilder(queryConfig).setJobId(jobId).build());

      // Wait for the query to complete.
      queryJob = queryJob.waitFor();

      // Check for errors
      if (queryJob == null) {
        throw new RuntimeException("Job no longer exists");
      } else if (queryJob.getStatus().getError() != null) {
        throw new RuntimeException(queryJob.getStatus().getError().toString());
      }

      // Get the results
      TableResult result = queryJob.getQueryResults();

      // Print the results
      for (FieldValueList row : result.iterateAll()) {
        String word = row.get("word").getStringValue();
        long wordCount = row.get("word_count").getLongValue();
        System.out.printf("word: %s, word_count: %d%n", word, wordCount);
      }
    }
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ValidationConnection arguments) throws Exception {
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
    return new BigQueryHandle(bigQuery);
  }
}
