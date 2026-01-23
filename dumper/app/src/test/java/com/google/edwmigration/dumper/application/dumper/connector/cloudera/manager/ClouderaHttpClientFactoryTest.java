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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.unauthorized;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.net.URI;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClouderaHttpClientFactoryTest {

  private static WireMockServer server;

  @BeforeClass
  public static void beforeClass() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void afterClass() {
    server.stop();
  }

  @Before
  public void setUp() {
    server.resetAll();
  }

  @Test
  public void createClouderaManagerClient_success() throws Exception {
    // 1. Mock login
    server.stubFor(post("/j_spring_security_check").willReturn(ok()));
    // 2. Mock home page check
    server.stubFor(get("/cmf/home").willReturn(ok()));

    URI apiUri = URI.create(server.baseUrl() + "/api/v41/");

    // Act
    try (CloseableHttpClient client =
        ClouderaHttpClientFactory.createClouderaManagerClient(apiUri, "user", "pass")) {

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

    URI apiUri = URI.create(server.baseUrl() + "/api/v41/");

    // Act & Assert
    assertThrows(
        MetadataDumperUsageException.class,
        () -> ClouderaHttpClientFactory.createClouderaManagerClient(apiUri, "user", "pass"));
  }

  @Test
  public void createBasicAuthClient_success() throws Exception {
    String user = "user";
    String pass = "pass";
    server.stubFor(get("/test-basic").willReturn(ok()));

    // Act
    try (CloseableHttpClient client = ClouderaHttpClientFactory.createBasicAuthClient(user, pass)) {
      assertNotNull(client);

      // Verify basic auth header is sent
      try (CloseableHttpResponse response =
          client.execute(new HttpGet(server.baseUrl() + "/test-basic"))) {
        assertEquals(200, response.getStatusLine().getStatusCode());
      }

      server.verify(
          getRequestedFor(urlEqualTo("/test-basic"))
              .withHeader("Authorization", equalTo("Basic dXNlcjpwYXNz")));
    }
  }
}
