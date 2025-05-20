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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RangerClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RangerClient.class);

  private static final ObjectReader MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .registerModule(new GuavaModule())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
          .reader();

  public static class RangerException extends RuntimeException {

    public RangerException(String message, Throwable cause) {
      super(message, cause);
    }

    public RangerException(String message) {
      super(message);
    }
  }

  private final ConnectionWrapper httpClient;

  RangerClient(ConnectionWrapper httpClient) {
    Preconditions.checkNotNull(httpClient, "Http client must not be null.");

    this.httpClient = httpClient;
  }

  static RangerClient instance(@Nonnull ConnectorArguments arguments) {
    URIBuilder uriBuilder =
        new URIBuilder().setPort(arguments.getPort()).setHost(arguments.getHostOrDefault());
    if (Objects.equals(arguments.getRangerScheme(), "https")) {
      uriBuilder.setScheme("https");
    } else {
      uriBuilder.setScheme("http");
    }
    ConnectionWrapper httpClient = createHttpClient(arguments, uriBuilder);
    return new RangerClient(httpClient);
  }

  public ImmutableList<Object> findUsers(Map<String, String> args) throws RangerException {
    return doHttpGet("/service/xusers/users", args, "vXUsers");
  }

  public ImmutableList<Object> findGroups(Map<String, String> args) throws RangerException {
    return doHttpGet("/service/xusers/groups", args, "vXGroups");
  }

  public ImmutableList<Object> findRoles(Map<String, String> args) throws RangerException {
    return doHttpGet("/service/roles/roles", args, "roles");
  }

  public ImmutableList<Object> findServices(Map<String, String> args) throws RangerException {
    return doHttpGet("/service/plugins/services", args, "services");
  }

  public ImmutableList<Object> findPolicies(Map<String, String> args) throws RangerException {
    return doHttpGet("/service/plugins/policies", args, "policies");
  }

  private ImmutableList<Object> doHttpGet(
      String path, Map<String, String> params, String payloadProperty) throws RangerException {
    String responseBody = httpClient.doGet(path, params);
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> payload = MAPPER.readValue(responseBody, Map.class);
      if (!payload.containsKey(payloadProperty)) {
        throw new RangerException("Missing key " + payloadProperty + " in Ranger response");
      }
      Object items = payload.get(payloadProperty);
      if (!(items instanceof List)) {
        throw new RangerException(
            "Expected an array value for key "
                + payloadProperty
                + " in Ranger response, got "
                + items.getClass());
      }
      return ImmutableList.copyOf((List<?>) items);
    } catch (IOException e) {
      throw new RangerException("Failed to deserialize Ranger internal API response body", e);
    }
  }

  @Override
  public void close() throws Exception {}

  static ConnectionWrapper createHttpClient(ConnectorArguments arguments, URIBuilder baseUri) {
    ConnectionWrapper.Builder clientBuilder = ConnectionWrapper.builder(baseUri);
    if (arguments.hasRangerIgnoreTlsValidation() && arguments.getRangerScheme().equals("https")) {
      clientBuilder.disableTlsValidation();
    }

    if (arguments.useKerberosAuthForHadoop()) {
      SecurityManager securityManager = System.getSecurityManager();
      if (securityManager != null) {
        logger.warn(
            "The application is running under security manager {}. It may need additional permissions to operate properly.",
            securityManager.getClass().getName());
      }
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      clientBuilder.withKerberosAuth();
    } else if (!Strings.isNullOrEmpty(arguments.getUser())) {
      String user = arguments.getUser();
      String password = arguments.getPasswordOrPrompt();

      clientBuilder.withBasicAuth(
          "Basic "
              + Base64.getEncoder()
                  .encodeToString(
                      String.format("%s:%s", user, password).getBytes(StandardCharsets.UTF_8)));
    }
    return clientBuilder.build();
  }

  static class ConnectionWrapper {

    private final String basicAuth;
    private final URIBuilder baseUri;
    private final SSLSocketFactory socketFactory;
    private final HostnameVerifier hostnameVerifier;
    private final boolean useKerberosAuth;

    static class Builder {
      private String basicAuth;
      private final URIBuilder baseUri;
      private SSLSocketFactory socketFactory;
      private HostnameVerifier hostnameVerifier;
      private boolean useKerberosAuth = false;

      Builder(URIBuilder baseUri) {
        this.baseUri = baseUri;
      }

      Builder withBasicAuth(String basicAuth) {
        this.basicAuth = basicAuth;
        return this;
      }

      ConnectionWrapper build() {
        return new ConnectionWrapper(
            baseUri, basicAuth, socketFactory, hostnameVerifier, useKerberosAuth);
      }

      public Builder disableTlsValidation() {
        try {
          TrustManager[] trustAllCerts =
              new TrustManager[] {
                new X509TrustManager() {
                  public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                  }

                  public void checkClientTrusted(
                      java.security.cert.X509Certificate[] certs, String authType) {}

                  public void checkServerTrusted(
                      java.security.cert.X509Certificate[] certs, String authType) {}
                }
              };
          SSLContext sc = SSLContext.getInstance("SSL"); // Or TLS
          sc.init(null, trustAllCerts, new java.security.SecureRandom());
          socketFactory = sc.getSocketFactory(); // Apply globally (affects all connections)

          hostnameVerifier = (hostname, session) -> true;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
          throw new RangerException("Cannot disable tls validation", e);
        }
        return this;
      }

      public Builder withKerberosAuth() {
        useKerberosAuth = true;
        return this;
      }
    }

    public static Builder builder(URIBuilder baseUri) {
      return new Builder(baseUri);
    }

    private ConnectionWrapper(
        URIBuilder baseUri,
        String basicAuth,
        SSLSocketFactory socketFactory,
        HostnameVerifier hostnameVerifier,
        boolean useKerberosAuth) {
      this.basicAuth = basicAuth;
      this.baseUri = baseUri;
      this.socketFactory = socketFactory;
      this.hostnameVerifier = hostnameVerifier;
      this.useKerberosAuth = useKerberosAuth;
    }

    public String doGet(String path, Map<String, String> queryParams) {
      URL url = constructUrl(path, queryParams);
      HttpURLConnection connection = null;
      try {
        connection = setupConnection(url);

        try {
          handleHttpErrors(connection, url);

          InputStream responseInputStream = connection.getInputStream();
          return getResponseBody(responseInputStream);
        } catch (IOException e) {
          throw new RangerException("Problem reading response", e);
        }
      } catch (ProtocolException e) {
        throw new RangerException("Problem with protocol " + baseUri.getScheme(), e);
      } catch (IOException e) {
        throw new RangerException("Problem opening http(s) connection to " + url, e);
      } catch (AuthenticationException e) {
        throw new RangerException("Problem with kerberos authentication", e);
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    private HttpURLConnection setupConnection(URL url) throws IOException, AuthenticationException {
      HttpURLConnection connection;
      if (useKerberosAuth) {
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        KerberosAuthenticator kerberosAuthenticator = new KerberosAuthenticator();
        AuthenticatedURL authenticatedURL = new AuthenticatedURL(kerberosAuthenticator);
        connection = authenticatedURL.openConnection(url, token);
      } else {
        connection = (HttpURLConnection) url.openConnection();
      }
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Accept", "application/json");
      if (basicAuth != null) {
        connection.setRequestProperty("Authorization", basicAuth);
      }
      if (socketFactory != null) {
        ((HttpsURLConnection) connection).setSSLSocketFactory(socketFactory);
        ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
      }
      return connection;
    }

    private void handleHttpErrors(HttpURLConnection connection, URL url) throws IOException {
      int responseCode = connection.getResponseCode();
      // handle errors
      if (responseCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
        InputStream inputStream = connection.getErrorStream();
        StringBuilder errorMessage =
            new StringBuilder(
                String.format(
                    "Ranger server returned error code %d when calling %s", responseCode, url));
        if (inputStream != null) {
          try {
            String errorBody = getResponseBody(inputStream);
            errorMessage.append(errorBody);
          } catch (IOException e) {
            logger.error("Failed to read error stream", e);
          }
        }
        throw new RangerException(errorMessage.toString());
      }
    }

    private URL constructUrl(String path, Map<String, String> queryParams) {
      URL url;
      try {
        url =
            baseUri
                .setPath(path)
                .addParameters(
                    queryParams.entrySet().stream()
                        .map(entry -> new BasicNameValuePair(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()))
                .build()
                .toURL();
      } catch (Exception ex) {
        throw new RangerException("Failed to build URL", ex);
      }
      return url;
    }

    private String getResponseBody(InputStream inputStream) throws IOException {
      StringBuilder body = new StringBuilder();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          body.append(line).append(System.lineSeparator());
        }
      }
      return body.toString();
    }
  }
}
