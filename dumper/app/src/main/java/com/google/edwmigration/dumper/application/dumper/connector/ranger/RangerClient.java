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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RangerConnector.class);

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

  private final UsernamePasswordCredentials credentials;

  public RangerClient(CloseableHttpClient httpClient, URI baseUri, String user, String password) {
    Preconditions.checkNotNull(httpClient, "Http client must not be null.");
    Preconditions.checkNotNull(baseUri, "BaseUri must not be null.");

    this.httpClient = httpClient;
    this.baseUri = baseUri;
    credentials = new UsernamePasswordCredentials(user, password);
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
    try {
      httpRequest.addHeader(new BasicScheme().authenticate(credentials, httpRequest));
    } catch (AuthenticationException e) {
      throw new RangerException("Failed to initialize authentication header", e);
    }
    try (CloseableHttpResponse httpResponse = httpClient.execute(httpRequest)) {
      LOG.debug("Response from Ranger({}): {}", path, httpResponse);
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
}
