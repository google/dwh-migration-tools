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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.temporaryRedirect;
import static com.github.tomakehurst.wiremock.client.WireMock.unauthorized;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.exception.SparkHistoryConnectionException;
import java.net.URI;
import javax.net.ssl.SSLException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClouderaHttpClientFactoryTest {

  private static final String BASIC_AUTH_HEADER = "Basic dXNlcjpwYXNz";

  /** The configured Cloudera Manager host. Serves HTTP and HTTPS with a self-signed certificate. */
  private static WireMockServer server;

  /** A different host, reached as 127.0.0.1 while the configured host is localhost. */
  private static WireMockServer otherServer;

  @BeforeClass
  public static void beforeClass() {
    server =
        new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().dynamicHttpsPort());
    server.start();
    otherServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    otherServer.start();
  }

  @AfterClass
  public static void afterClass() {
    server.stop();
    otherServer.stop();
  }

  @Before
  public void setUp() {
    server.resetAll();
    otherServer.resetAll();
  }

  private static String baseUrl() {
    return "http://localhost:" + server.port();
  }

  private static URI apiUri() {
    return URI.create(baseUrl() + "/api/v41/");
  }

  private static URI httpsApiUri() {
    return URI.create("https://localhost:" + server.httpsPort() + "/api/v41/");
  }

  private static String otherHostUrl(String path) {
    return "http://127.0.0.1:" + otherServer.port() + path;
  }

  private static int executeGet(CloseableHttpClient client, String url) throws Exception {
    try (CloseableHttpResponse response = client.execute(new HttpGet(url))) {
      return response.getStatusLine().getStatusCode();
    }
  }

  @Test
  public void createClouderaManagerClient_success() throws Exception {
    // 1. Mock login
    server.stubFor(post("/j_spring_security_check").willReturn(ok()));
    // 2. Mock home page check
    server.stubFor(get("/cmf/home").willReturn(ok()));

    // Act
    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createClouderaManagerClient(apiUri(), "user", "pass", false)) {

      // Assert
      assertNotNull(client);
      server.verify(
          postRequestedFor(urlEqualTo("/j_spring_security_check"))
              .withRequestBody(equalTo("j_username=user&j_password=pass")));
      server.verify(getRequestedFor(urlEqualTo("/cmf/home")));
    }
  }

  @Test
  public void createClouderaManagerClient_loginFails_throwsException() throws Exception {
    // 1. Mock login success (POST returns 200)
    server.stubFor(post("/j_spring_security_check").willReturn(ok()));
    // 2. Mock home page check failure (session not established)
    server.stubFor(get("/cmf/home").willReturn(unauthorized()));

    // Act & Assert
    assertThrows(
        MetadataDumperUsageException.class,
        () ->
            ClouderaHttpClientFactory.createClouderaManagerClient(apiUri(), "user", "pass", false));
  }

  @Test
  public void createClouderaManagerClient_selfSignedCertificate_rejectedByDefault() {
    server.stubFor(post("/j_spring_security_check").willReturn(ok()));
    server.stubFor(get("/cmf/home").willReturn(ok()));

    assertThrows(
        SSLException.class,
        () ->
            ClouderaHttpClientFactory.createClouderaManagerClient(
                httpsApiUri(), "user", "pass", false));
    server.verify(0, anyRequestedFor(anyUrl()));
  }

  @Test
  public void createClouderaManagerClient_selfSignedCertificate_acceptedWithFlag()
      throws Exception {
    server.stubFor(post("/j_spring_security_check").willReturn(ok()));
    server.stubFor(get("/cmf/home").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createClouderaManagerClient(
            httpsApiUri(), "user", "pass", true)) {
      server.verify(postRequestedFor(urlEqualTo("/j_spring_security_check")));
      server.verify(getRequestedFor(urlEqualTo("/cmf/home")));
    }
  }

  @Test
  public void createBasicAuthClient_success() throws Exception {
    server.stubFor(get("/test-basic").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      assertEquals(200, executeGet(client, baseUrl() + "/test-basic"));
    }

    server.verify(
        getRequestedFor(urlEqualTo("/test-basic"))
            .withHeader("Authorization", equalTo(BASIC_AUTH_HEADER)));
  }

  @Test
  public void createBasicAuthClient_selfSignedCertificate_rejectedByDefault() throws Exception {
    server.stubFor(get("/test-basic").willReturn(ok()));
    String url = "https://localhost:" + server.httpsPort() + "/test-basic";

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(httpsApiUri(), "user", "pass", false)) {
      assertThrows(SSLException.class, () -> executeGet(client, url));
    }
    server.verify(0, anyRequestedFor(anyUrl()));
  }

  @Test
  public void createBasicAuthClient_selfSignedCertificate_acceptedWithFlag() throws Exception {
    server.stubFor(get("/test-basic").willReturn(ok()));
    String url = "https://localhost:" + server.httpsPort() + "/test-basic";

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(httpsApiUri(), "user", "pass", true)) {
      assertEquals(200, executeGet(client, url));
    }
    server.verify(
        getRequestedFor(urlEqualTo("/test-basic"))
            .withHeader("Authorization", equalTo(BASIC_AUTH_HEADER)));
  }

  @Test
  public void createBasicAuthClient_otherHost_doesNotReceiveCredentials() throws Exception {
    otherServer.stubFor(get("/other").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      assertEquals(200, executeGet(client, otherHostUrl("/other")));
    }

    otherServer.verify(getRequestedFor(urlEqualTo("/other")).withoutHeader("Authorization"));
  }

  @Test
  public void createBasicAuthClient_otherPortOnConfiguredHost_doesNotReceiveCredentials()
      throws Exception {
    otherServer.stubFor(get("/other").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      assertEquals(200, executeGet(client, "http://localhost:" + otherServer.port() + "/other"));
    }

    otherServer.verify(getRequestedFor(urlEqualTo("/other")).withoutHeader("Authorization"));
  }

  @Test
  public void createBasicAuthClient_redirectToOtherHost_notFollowed() throws Exception {
    server.stubFor(get("/redirect").willReturn(temporaryRedirect(otherHostUrl("/other"))));
    otherServer.stubFor(get("/other").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      assertThrows(
          ClientProtocolException.class, () -> executeGet(client, baseUrl() + "/redirect"));
    }

    otherServer.verify(0, anyRequestedFor(anyUrl()));
  }

  @Test
  public void createBasicAuthClient_sparkLogRedirectToOtherHost_notFollowed() throws Exception {
    String logPath = "/gateway/cdp-proxy-api/spark3history/api/v1/applications/app1/logs";
    server.stubFor(
        get(logPath)
            .willReturn(
                aResponse().withStatus(302).withHeader("Location", otherHostUrl("/other"))));
    otherServer.stubFor(get("/other").willReturn(okJson("{}")));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      SparkJobMetadataExtractor extractor =
          new SparkJobMetadataExtractor(new ObjectMapper(), client);
      Exception thrown =
          assertThrows(
              Exception.class,
              () -> extractor.extract(URI.create(baseUrl() + logPath), "cluster1", "app1"));

      server.verify(
          getRequestedFor(urlEqualTo(logPath))
              .withHeader("Authorization", equalTo(BASIC_AUTH_HEADER)));
      otherServer.verify(0, anyRequestedFor(anyUrl()));
      assertTrue(thrown instanceof SparkHistoryConnectionException);
    }
  }

  @Test
  public void createBasicAuthClient_redirectOnConfiguredHost_followedWithCredentials()
      throws Exception {
    server.stubFor(get("/redirect").willReturn(temporaryRedirect("/target")));
    server.stubFor(get("/target").willReturn(ok()));

    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createBasicAuthClient(apiUri(), "user", "pass", false)) {
      assertEquals(200, executeGet(client, baseUrl() + "/redirect"));
    }

    server.verify(
        getRequestedFor(urlEqualTo("/target"))
            .withHeader("Authorization", equalTo(BASIC_AUTH_HEADER)));
  }

  @Test
  public void isSameHostOrDomain_sameHostOrSameDomain_returnsTrue() {
    assertTrue(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "cm.example.com"));
    assertTrue(ClouderaHttpClientFactory.isSameHostOrDomain("CM.Example.com", "cm.example.COM."));
    assertTrue(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "knox.example.com"));
    assertTrue(
        ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "knox.dc1.example.com"));
    assertTrue(
        ClouderaHttpClientFactory.isSameHostOrDomain(
            "cm.dc1.example.com", "knox_1.dc1.example.com"));
    assertTrue(ClouderaHttpClientFactory.isSameHostOrDomain("localhost", "localhost"));
    assertTrue(ClouderaHttpClientFactory.isSameHostOrDomain("10.0.0.5", "10.0.0.5"));
  }

  @Test
  public void isSameHostOrDomain_otherHost_returnsFalse() {
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "knox.example.org"));
    assertFalse(
        ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "knox.otherexample.com"));
    assertFalse(
        ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "example.com.other.net"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "example.com"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "10.0.0.5"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("10.0.0.5", "knox.example.com"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("10.0.0.5", "10.0.0.6"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("localhost", "knox"));
    assertFalse(
        ClouderaHttpClientFactory.isSameHostOrDomain("cm.dc1.example.com", "knox.example.com"));
    // A single-label domain is not a cluster domain.
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.com", "other.com"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("10.0.0.5", "1.0.0.5"));
    assertFalse(
        ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", "other.net/.example.com"));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", ""));
    assertFalse(ClouderaHttpClientFactory.isSameHostOrDomain("cm.example.com", null));
  }
}
