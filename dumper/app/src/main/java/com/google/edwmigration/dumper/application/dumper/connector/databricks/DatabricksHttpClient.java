/*
 * Minimal HTTP client wrapper for Databricks REST API (skeleton).
 * Implement paging, retries and authentication here when developing the connector.
 */
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public final class DatabricksHttpClient {
  private final OkHttpClient client;
  private final String host;
  private final String bearerToken;
  private final ObjectMapper mapper = new ObjectMapper();

  public DatabricksHttpClient(String host, String bearerToken) {
    this.client = new OkHttpClient();
    this.host = host;
    this.bearerToken = bearerToken;
  }

  public JsonNode get(String path) throws IOException {
    HttpUrl url = HttpUrl.parse(host).newBuilder().encodedPath(path).build();
    Request request =
        new Request.Builder()
            .url(url)
            .header("Authorization", "Bearer " + bearerToken)
            .header("Accept", "application/json")
            .get()
            .build();
    try (Response resp = client.newCall(request).execute()) {
      if (!resp.isSuccessful()) {
        throw new IOException("Unexpected response code: " + resp.code());
      }
      return mapper.readTree(resp.body().byteStream());
    }
  }
}
