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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.edwmigration.validation.connector.api.Handle;
import java.io.IOException;
import org.apache.commons.io.FileUtils;

public class BigQueryHandle implements Handle {
  private final BigQuery bigQuery;

  private BigQueryHandle(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  public BigQuery getBigQuery() {
    return this.bigQuery;
  }

  @Override
  public void close() {
    // No-op for now; BigQuery client handles its own resources.
  }

  // should this become the open method on the connector now?
  public static BigQuery create(String projectId) {
    String credentialsFile = System.getenv(ServiceOptions.CREDENTIAL_ENV_NAME);
    try {
      BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();

      if (credentialsFile != null) {
        builder.setCredentials(
            GoogleCredentials.fromStream(
                FileUtils.openInputStream(FileUtils.getFile(credentialsFile))));
      }

      return builder.setProjectId(projectId).build().getService();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create BigQuery client", e);
    }
  }
}
