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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/** Utility class for verifying the preconditions and configuration of a Cloudera Connector. */
public final class ClouderaConnectorVerifier {

  private ClouderaConnectorVerifier() {}

  /**
   * Verifies the validity of the connector configuration and environment.
   *
   * <p>This involves checking connectivity to the API, verifying the existence of the specified
   * cluster (if provided), and ensuring other preconditions are met.
   *
   * @param handle The handle to the Cloudera Manager API.
   * @param arguments The connector arguments containing target cluster details.
   * @throws ClouderaConnectorException If verification fails due to missing resources, API errors,
   *     or connectivity issues.
   */
  public static void verify(
      @Nonnull ClouderaManagerHandle handle, @Nonnull ConnectorArguments arguments)
      throws ClouderaConnectorException {
    verifyClusterExists(handle, arguments.getCluster());
  }

  private static void verifyClusterExists(ClouderaManagerHandle handle, String clusterName)
      throws ClouderaConnectorException {
    if (clusterName == null) {
      return;
    }
    String endpoint = String.format("%s/clusters/%s", handle.getApiURI(), clusterName);
    HttpGet httpGet = new HttpGet(endpoint);

    try (CloseableHttpResponse response = handle.getHttpClient().execute(httpGet)) {

      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode == 404) {
        throw new ClouderaConnectorException(
            String.format("Specified cluster '%s' not found.", clusterName));
      }

      if (!isHttpStatusSuccess(statusCode)) {
        String errorMsg = EntityUtils.toString(response.getEntity());
        throw new ClouderaConnectorException(
            String.format(
                "Unexpected API error checking cluster '%s'. Code: %d. Message: %s",
                clusterName, statusCode, errorMsg));
      }
    } catch (IOException e) {
      throw new ClouderaConnectorException(
          String.format(
              "Failed to communicate with Cloudera Manager API while checking cluster '%s'.",
              clusterName),
          e);
    }
  }

  private static boolean isHttpStatusSuccess(int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }
}
