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
import com.google.edwmigration.validation.ValidationConnection;
import com.google.edwmigration.validation.connector.AbstractConnector;
import com.google.edwmigration.validation.handle.AbstractHandle;
import com.google.edwmigration.validation.handle.Handle;
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
