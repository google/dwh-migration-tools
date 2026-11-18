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
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.net.ssl.SSLException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
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
 * <p>Both clients verify TLS certificates and host names against the JVM default trust store. With
 * {@code --cloudera-disable-tls-validation} they accept any certificate instead.
 */
public final class ClouderaHttpClientFactory {

  private static final Logger logger = LoggerFactory.getLogger(ClouderaHttpClientFactory.class);

  /**
   * Connection Timeout (5 seconds) Time to wait for the TCP handshake. Since Cloudera clusters are
   * on internal networks, if a host is reachable, it should connect almost instantly. If it takes
   * >5s, the host is likely down or firewalled, and we shouldn't block the thread longer.
   */
  private static final int CONNECTION_TIMEOUT_MS = 5000;

  /**
   * Socket Timeout (30 seconds) Time to wait for data packets after connection. Spark History
   * Server and YARN are heavy Java applications prone to Garbage Collection pauses and slow I/O
   * (parsing large HDFS logs). We give them 30s to respond before giving up.
   */
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
   * @param disableTlsValidation whether to accept any TLS certificate and host name
   * @return a ready-to-use, authenticated http client
   * @throws Exception if the login request fails, the credentials are invalid, or the home page is
   *     inaccessible
   */
  public static CloseableHttpClient createClouderaManagerClient(
      URI apiUri, String username, String password, boolean disableTlsValidation) throws Exception {

    HttpClientBuilder builder = HttpClients.custom();
    if (disableTlsValidation) {
      configureTrustAllSSL(builder);
    }
    CloseableHttpClient client = builder.build();

    try {
      authenticateViaForm(apiUri, client, username, password);
    } catch (Exception e) {
      if (e instanceof SSLException) {
        logger.error(
            "TLS validation failed for Cloudera Manager at '{}'. Add its certificate to the JVM"
                + " trust store, or use --{} to disable validation.",
            apiUri.getHost(),
            ConnectorArguments.OPT_CLOUDERA_DISABLE_TLS_VALIDATION);
      }
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
   * <p>This client is isolated from the application's global state. It is suitable for stateless
   * REST APIs such as the Spark History Server or YARN ResourceManager.
   *
   * <p>The credentials are sent only to the Cloudera Manager scheme, host and port of {@code
   * apiUri}, and over HTTPS to hosts that pass {@link #isSameHostOrDomain}. Redirects to any other
   * target are not followed.
   *
   * <p>Timeouts are explicitly configured (5s connect, 30s read) to handle potential latency in
   * overloaded cluster services.
   *
   * @param apiUri the API URI of the Cloudera Manager instance, given by the user
   * @param username the username for the Basic Auth header
   * @param password the password for the Basic Auth header
   * @param disableTlsValidation whether to accept any TLS certificate and host name
   * @return a configured http client that sends credentials only to the allowed hosts
   * @throws Exception if SSL configuration fails
   */
  public static CloseableHttpClient createBasicAuthClient(
      URI apiUri, String username, String password, boolean disableTlsValidation) throws Exception {
    HttpClientBuilder builder = HttpClients.custom();
    if (disableTlsValidation) {
      configureTrustAllSSL(builder);
    }

    String authHeader =
        "Basic "
            + java.util.Base64.getEncoder()
                .encodeToString(
                    (username + ":" + password).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    // Interceptors run again for every redirected request, with that request's target host.
    builder.addInterceptorLast(
        (HttpRequestInterceptor)
            (request, context) -> {
              request.removeHeaders(HttpHeaders.AUTHORIZATION);
              HttpHost target = HttpClientContext.adapt(context).getTargetHost();
              if (isCredentialTarget(apiUri, target)) {
                request.addHeader(HttpHeaders.AUTHORIZATION, authHeader);
              }
            });
    builder.setRedirectStrategy(new CredentialTargetRedirectStrategy(apiUri));

    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(CONNECTION_TIMEOUT_MS)
            .setSocketTimeout(SOCKET_TIMEOUT_MS)
            .setCircularRedirectsAllowed(true) // Helpful for some SSO redirects
            .build();
    builder.setDefaultRequestConfig(config);

    return builder.build();
  }

  /**
   * Returns whether {@code host} is the configured host or a host in its domain. The domain is the
   * configured host name without its first label, for example {@code dc1.example.com} for {@code
   * cm.dc1.example.com}, and must have at least two labels. IP addresses and host names with fewer
   * than three labels only match themselves.
   */
  static boolean isSameHostOrDomain(
      @CheckForNull String configuredHost, @CheckForNull String host) {
    if (configuredHost == null || host == null) {
      return false;
    }
    String configured = normalizeHost(configuredHost);
    String candidate = normalizeHost(host);
    if (candidate.isEmpty()) {
      return false;
    }
    if (candidate.equals(configured)) {
      return true;
    }
    if (!isDnsName(configured) || !isDnsName(candidate)) {
      return false;
    }
    String domain = configured.substring(configured.indexOf('.') + 1);
    if (domain.equals(configured) || domain.indexOf('.') < 0) {
      return false;
    }
    return candidate.endsWith("." + domain);
  }

  private static final Pattern DNS_NAME = Pattern.compile("[a-z0-9_-]+(\\.[a-z0-9_-]+)*");

  private static final Pattern NUMERIC_LABEL = Pattern.compile(".*\\.[0-9]+|[0-9]+");

  /** Returns whether a normalized host is a DNS name, and not an IP address. */
  private static boolean isDnsName(String host) {
    return DNS_NAME.matcher(host).matches() && !NUMERIC_LABEL.matcher(host).matches();
  }

  /**
   * Returns whether the Basic Auth credentials may be sent to {@code target}: the scheme, host and
   * port of {@code apiUri}, or HTTPS to a host that passes {@link #isSameHostOrDomain}.
   */
  private static boolean isCredentialTarget(URI apiUri, @CheckForNull HttpHost target) {
    if (target == null) {
      return false;
    }
    String scheme = target.getSchemeName();
    String host = target.getHostName();
    boolean isConfiguredTarget =
        scheme.equalsIgnoreCase(apiUri.getScheme())
            && normalizeHost(host).equals(normalizeHost(apiUri.getHost()))
            && portOrDefault(scheme, target.getPort())
                == portOrDefault(apiUri.getScheme(), apiUri.getPort());
    return isConfiguredTarget
        || ("https".equalsIgnoreCase(scheme) && isSameHostOrDomain(apiUri.getHost(), host));
  }

  private static String normalizeHost(@CheckForNull String host) {
    if (host == null) {
      return "";
    }
    String result = host.toLowerCase(Locale.ROOT);
    if (result.startsWith("[") && result.endsWith("]")) {
      result = result.substring(1, result.length() - 1);
    }
    if (result.endsWith(".")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  private static int portOrDefault(String scheme, int port) {
    if (port >= 0) {
      return port;
    }
    return "https".equalsIgnoreCase(scheme) ? 443 : 80;
  }

  /** Follows a redirect only if its target may receive the credentials. */
  private static final class CredentialTargetRedirectStrategy extends DefaultRedirectStrategy {

    private final URI apiUri;

    CredentialTargetRedirectStrategy(URI apiUri) {
      this.apiUri = apiUri;
    }

    @Override
    public HttpUriRequest getRedirect(
        HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
      HttpUriRequest redirect = super.getRedirect(request, response, context);
      if (!isCredentialTarget(apiUri, URIUtils.extractHost(redirect.getURI()))) {
        throw new RedirectException(
            "Not following a redirect to host '"
                + redirect.getURI().getHost()
                + "', which is outside the Cloudera Manager host and domain.");
      }
      return redirect;
    }
  }

  private static HttpClientBuilder configureTrustAllSSL(HttpClientBuilder builder)
      throws Exception {
    logger.warn("TLS certificate and host name validation is disabled for Cloudera connections.");
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
