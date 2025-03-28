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
import static org.apache.http.HttpHeaders.ACCEPT;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
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

  public static class RangerException extends Exception {

    public RangerException(String message, Throwable cause) {
      super(message, cause);
    }

    public RangerException(String message) {
      super(message);
    }
  }

  private final URI baseUri;

  private final CloseableHttpClient httpClient;

  RangerClient(CloseableHttpClient httpClient, URI baseUri) {
    Preconditions.checkNotNull(httpClient, "Http client must not be null.");
    Preconditions.checkNotNull(baseUri, "BaseUri must not be null.");

    this.httpClient = httpClient;
    this.baseUri = baseUri;
  }

  static RangerClient instance(@Nonnull ConnectorArguments arguments) throws Exception {
    URIBuilder uriBuilder =
        new URIBuilder().setPort(arguments.getPort()).setHost(arguments.getHostOrDefault());
    if (Objects.equals(arguments.getRangerScheme(), "https")) {
      uriBuilder.setScheme("https");
    } else {
      uriBuilder.setScheme("http");
    }
    URI apiUrl = uriBuilder.build();
    CloseableHttpClient httpClient = createHttpClient(arguments);
    return new RangerClient(httpClient, apiUrl);
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
    URI uri;
    try {
      uri =
          new URIBuilder(baseUri)
              .setPath(path)
              .addParameters(
                  params.entrySet().stream()
                      .map(entry -> new BasicNameValuePair(entry.getKey(), entry.getValue()))
                      .collect(toImmutableList()))
              .build();
    } catch (URISyntaxException e) {
      throw new RangerException("Failed to build API URI", e);
    }
    HttpGet httpRequest = new HttpGet(uri);
    httpRequest.addHeader(ACCEPT, APPLICATION_JSON.toString());
    try (CloseableHttpResponse httpResponse = httpClient.execute(httpRequest)) {
      logger.debug("Response from Ranger({}): {}", path, httpResponse);
      if (httpResponse.getStatusLine().getStatusCode() != SC_OK) {
        throw new RangerException(
            String.format(
                "Failed to fetch data from Ranger internal API: '%s'",
                httpResponse.getStatusLine()));
      }
      HttpEntity entity = httpResponse.getEntity();
      try {
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = MAPPER.readValue(EntityUtils.toString(entity), Map.class);
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
    } catch (IOException e) {
      throw new RangerException("Failed to connect to Ranger API", e);
    }
  }

  @Override
  public void close() throws Exception {
    httpClient.close();
  }

  private static CloseableHttpClient createHttpClient(ConnectorArguments arguments)
      throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
    HttpClientBuilder clientBuilder = HttpClients.custom();
    if (arguments.hasRangerIgnoreTlsValidation()) {
      // create a client with a trust strategy which accepts any certificate
      TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
      SSLContext sslContext =
          SSLContextBuilder.create().loadTrustMaterial(null, acceptingTrustStrategy).build();
      clientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    }

    if (arguments.useKerberosAuthForHadoop()) {
      Registry<AuthSchemeProvider> authSchemeRegistry =
          RegistryBuilder.<AuthSchemeProvider>create()
              .register(
                  "Negotiate",
                  new SPNegoSchemeFactory(true)) // 'true' enables use of native GSS-API
              .build();

      RequestConfig defaultRequestConfig =
          RequestConfig.custom()
              .setTargetPreferredAuthSchemes(Collections.singletonList("Negotiate"))
              .build();

      clientBuilder
          .setDefaultAuthSchemeRegistry(authSchemeRegistry)
          .setDefaultRequestConfig(defaultRequestConfig);
    } else if (!Strings.isNullOrEmpty(arguments.getUser())) {
      String user = arguments.getUser();
      String password = arguments.getPasswordOrPrompt();

      BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
      basicCredentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(user, password));
      clientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
    }
    return clientBuilder.build();
  }
}
