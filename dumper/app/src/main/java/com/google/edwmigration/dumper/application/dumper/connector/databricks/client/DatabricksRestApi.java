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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.http.Request;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads the responses of the Databricks REST API as JSON. The SDK models drop the fields that they
 * don't have, such as {@code unique_constraint} of {@code tables.get} or {@code
 * metrics.write_remote_rows} of the query history API in SDK 0.163.0, and read the enum values that
 * they don't know as null, so the calls that need every value send the request of the SDK and read
 * its JSON response.
 */
public final class DatabricksRestApi {

  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);

  /** Executes {@code request} with the headers of the SDK calls and returns its JSON response. */
  public static JsonNode executeJson(WorkspaceClient client, Request request) throws IOException {
    ApiClient apiClient = client.apiClient();
    request.withHeader("Accept", "application/json");
    if (apiClient.workspaceId() != null) {
      request.withHeader("X-Databricks-Workspace-Id", apiClient.workspaceId());
    }

    try (InputStream body = apiClient.execute(request, InputStream.class)) {
      return JSON_READER.readTree(body);
    }
  }

  private DatabricksRestApi() {}
}
