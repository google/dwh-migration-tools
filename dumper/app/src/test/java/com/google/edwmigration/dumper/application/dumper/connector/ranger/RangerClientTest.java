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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_CONNECTOR;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PASSWORD;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PORT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_RANGER_DISABLE_TLS_VALIDATION;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_RANGER_SCHEME;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_USER;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerClient.ConnectionWrapper;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerClient.RangerException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import org.apache.http.client.utils.URIBuilder;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RangerClientTest {

  private WireMockServer server;

  @After
  public void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void instance_noScheme_usesHttps() throws Exception {
    startHttpsServerWithUsers();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_CONNECTOR,
            "ranger",
            "--" + OPT_HOST,
            "localhost",
            "--" + OPT_PORT,
            String.valueOf(server.httpsPort()),
            "--" + OPT_USER,
            "dummy",
            "--" + OPT_PASSWORD,
            "dummy",
            "--" + OPT_RANGER_DISABLE_TLS_VALIDATION);

    ImmutableList<Object> users = RangerClient.instance(arguments).findUsers(ImmutableMap.of());

    assertTrue(users.isEmpty());
  }

  @Test
  public void instance_uppercaseHttpsScheme_usesHttps() throws Exception {
    startHttpsServerWithUsers();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_CONNECTOR,
            "ranger",
            "--" + OPT_HOST,
            "localhost",
            "--" + OPT_PORT,
            String.valueOf(server.httpsPort()),
            "--" + OPT_RANGER_SCHEME,
            "HTTPS",
            "--" + OPT_RANGER_DISABLE_TLS_VALIDATION);

    ImmutableList<Object> users = RangerClient.instance(arguments).findUsers(ImmutableMap.of());

    assertTrue(users.isEmpty());
  }

  @Test
  public void instance_unknownScheme_throws() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_CONNECTOR,
            "ranger",
            "--" + OPT_HOST,
            "localhost",
            "--" + OPT_PORT,
            "6080",
            "--" + OPT_RANGER_SCHEME,
            "ftp");

    assertThrows(MetadataDumperUsageException.class, () -> RangerClient.instance(arguments));
  }

  @Test
  public void setupConnection_setsDefaultTimeouts() throws Exception {
    ConnectionWrapper client =
        ConnectionWrapper.builder(
                new URIBuilder().setScheme("http").setHost("localhost").setPort(6080))
            .build();

    HttpURLConnection connection =
        client.setupConnection(new URL("http://localhost:6080/service/xusers/users"));

    assertEquals(ConnectionWrapper.CONNECT_TIMEOUT_MILLIS, connection.getConnectTimeout());
    assertEquals(ConnectionWrapper.READ_TIMEOUT_MILLIS, connection.getReadTimeout());
  }

  @Test(timeout = 10_000)
  public void doGet_unresponsiveServer_failsWithTimeout() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    server.stubFor(
        get(urlEqualTo("/service/xusers/users"))
            .willReturn(
                aResponse().withStatus(200).withBody("{\"vXUsers\":[]}").withFixedDelay(30_000)));
    ConnectionWrapper client =
        ConnectionWrapper.builder(
                new URIBuilder().setScheme("http").setHost("localhost").setPort(server.port()))
            .withTimeouts(/* connectTimeoutMillis= */ 1_000, /* readTimeoutMillis= */ 500)
            .build();

    RangerException e =
        assertThrows(
            RangerException.class, () -> client.doGet("/service/xusers/users", ImmutableMap.of()));

    assertThat(e.getCause(), instanceOf(SocketTimeoutException.class));
  }

  private void startHttpsServerWithUsers() {
    server =
        new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().dynamicHttpsPort());
    server.start();
    server.stubFor(
        get(urlEqualTo("/service/xusers/users"))
            .willReturn(aResponse().withStatus(200).withBody("{\"vXUsers\":[]}")));
  }
}
