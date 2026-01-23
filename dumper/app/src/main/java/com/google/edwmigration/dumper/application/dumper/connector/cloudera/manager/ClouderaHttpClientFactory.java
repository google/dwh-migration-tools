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

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating specialized {@link CloseableHttpClient} instances for Cloudera ecosystem
 * interactions.
 *
 * <p>This factory provides two distinct client types to handle the different authentication
 * mechanisms present in Cloudera environments:
 *
 * <ul>
 *   <li><b>Cloudera Manager Client:</b> Stateful, cookie-aware client that performs a Form-Based
 *       login handshake.
 *   <li><b>Basic Auth Client:</b> Stateless, isolated client for services like Spark History Server
 *       or YARN.
 * </ul>
 *
 * <p>Both clients are configured to trust all SSL certificates by default, as internal cluster
 * certificates are frequently self-signed.
 */
public final class ClouderaHttpClientFactory {

  private static final Logger logger = LoggerFactory.getLogger(ClouderaHttpClientFactory.class);

  private static final int CONNECTION_TIMEOUT_MS = 5000;
  private static final int SOCKET_TIMEOUT_MS = 30000;

  /**
   * Creates a {@link CloseableHttpClient} configured for Cloudera Manager and performs the login
   * handshake.
   *
   * <p>This method builds a client with cookie management enabled, executes the Spring Security
   * form login request (`/j_spring_security_check`), and verifies the session by accessing the home
   * page.
   *
   * @param apiUri the API URI of the Cloudera Manager instance
   * @param username the username for Cloudera Manager
   * @param password the password for Cloudera Manager
   * @return a ready-to-use, authenticated http client
   * @throws Exception if the login request fails, the credentials are invalid, or the home page is
   *     inaccessible
   */
  public static CloseableHttpClient createClouderaManagerClient(
      URI apiUri, String username, String password) throws Exception {

    HttpClientBuilder builder = HttpClients.custom();
    configureTrustAllSSL(builder);
    CloseableHttpClient client = builder.build();

    try {
      authenticateViaForm(apiUri, client, username, password);
    } catch (Exception e) {
      // If login fails, close the client to avoid leaking resources
      try {
        client.close();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }

    return client;
  }

  /**
   * Creates a dedicated {@link CloseableHttpClient} configured with Basic Authentication.
   *
   * <p>This client is isolated from the application's global state and uses a dedicated {@link
   * CredentialsProvider}. It is suitable for stateless REST APIs such as the Spark History Server
   * or YARN ResourceManager.
   *
   * <p>Timeouts are explicitly configured (5s connect, 30s read) to handle potential latency in
   * overloaded cluster services.
   *
   * @param username the username for the Basic Auth header
   * @param password the password for the Basic Auth header
   * @return a configured http client that sends credentials with every request
   * @throws Exception if SSL configuration fails
   */
  public static CloseableHttpClient createBasicAuthClient(String username, String password)
      throws Exception {
    HttpClientBuilder builder = HttpClients.custom();
    configureTrustAllSSL(builder);

    String authHeader =
        "Basic "
            + java.util.Base64.getEncoder()
                .encodeToString(
                    (username + ":" + password).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader));
    builder.setDefaultHeaders(headers);

    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(CONNECTION_TIMEOUT_MS)
            .setSocketTimeout(SOCKET_TIMEOUT_MS)
            .setCircularRedirectsAllowed(true) // Helpful for some SSO redirects
            .build();
    builder.setDefaultRequestConfig(config);

    return builder.build();
  }

  private static HttpClientBuilder configureTrustAllSSL(HttpClientBuilder builder)
      throws Exception {
    builder.setSSLContext(
        new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build());
    builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    return builder;
  }

  private static void authenticateViaForm(
      URI apiUri, CloseableHttpClient httpClient, String username, String password)
      throws Exception {
    URI baseUri = apiUri.resolve("/");
    HttpPost post = new HttpPost(baseUri + "/j_spring_security_check");
    List<NameValuePair> urlParameters = new ArrayList<>();
    urlParameters.add(new BasicNameValuePair("j_username", username));
    urlParameters.add(new BasicNameValuePair("j_password", password));

    post.setEntity(new UrlEncodedFormEntity(urlParameters));

    try (CloseableHttpResponse loginResponse = httpClient.execute(post)) {
      // Check Home Page to verify the session cookie is valid
      try (CloseableHttpResponse homeResponse =
          httpClient.execute(new HttpGet(baseUri + "/cmf/home"))) {
        if (HttpStatus.SC_OK != homeResponse.getStatusLine().getStatusCode()) {
          logger.error("Login failed. Home Page response: {}", homeResponse.getStatusLine());
          throw new MetadataDumperUsageException("Cloudera Manager login failed: " + loginResponse);
        }
      }
    }
    logger.info("Successfully logged into Cloudera Manager.");
  }
}
