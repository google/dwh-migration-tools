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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_CONNECTOR;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_HOST;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_OUTPUT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PASSWORD;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_PORT;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_RANGER_DISABLE_TLS_VALIDATION;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_RANGER_SCHEME;
import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_USER;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerClient.RangerException;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import javax.net.ssl.SSLHandshakeException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RangerConnectorTlsTest {
  private WireMockServer server;

  @After
  public void teardown() {
    server.stop();
  }

  @Test
  public void open_getOverHttpWorks() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().port(8080));
    server.start();
    configureFor("localhost", 8080);
    stubFor(
        get(urlEqualTo("/service/xusers/users"))
            .willReturn(aResponse().withStatus(200).withBody("{\"vXUsers\":[]}")));

    RangerConnector connector = new RangerConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_PORT, "8080",
            "--" + OPT_HOST, "localhost",
            "--" + OPT_RANGER_SCHEME, "http",
            "--" + OPT_PASSWORD, "dummy",
            "--" + OPT_USER, "dummy",
            "--" + OPT_CONNECTOR, "ranger",
            "--" + OPT_OUTPUT, "dummy");
    RangerClientHandle connectorHandle = (RangerClientHandle) connector.open(arguments);
    connectorHandle.rangerClient.findUsers(ImmutableMap.of());
  }

  @Test
  public void open_noIgnoreTlsVerification_failsWhenUsingSelfSignedTls() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().httpsPort(8443));
    server.start();
    configureFor("https", "localhost", 8443);
    stubFor(
        get(urlEqualTo("/service/xusers/users"))
            .willReturn(aResponse().withStatus(200).withBody("{\"vXUsers\":[]}")));

    RangerConnector connector = new RangerConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_PORT, "8443",
            "--" + OPT_HOST, "localhost",
            "--" + OPT_RANGER_SCHEME, "https",
            "--" + OPT_PASSWORD, "dummy",
            "--" + OPT_USER, "dummy",
            "--" + OPT_CONNECTOR, "ranger",
            "--" + OPT_OUTPUT, "dummy");
    RangerClientHandle connectorHandle = (RangerClientHandle) connector.open(arguments);
    RangerException ex =
        assertThrows(
            RangerException.class, () -> connectorHandle.rangerClient.findUsers(ImmutableMap.of()));
    assertThat(ex.getCause(), instanceOf(SSLHandshakeException.class));
  }

  @Test
  public void open_selfSignedTlsWorksWithIgnoreTlsVerificationFlag() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().httpsPort(8443));
    server.start();
    configureFor("https", "localhost", 8443);
    stubFor(
        get(urlEqualTo("/service/xusers/users"))
            .willReturn(aResponse().withStatus(200).withBody("{\"vXUsers\":[]}")));

    RangerConnector connector = new RangerConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--" + OPT_PORT,
            "8443",
            "--" + OPT_HOST,
            "localhost",
            "--" + OPT_RANGER_SCHEME,
            "https",
            "--" + OPT_PASSWORD,
            "dummy",
            "--" + OPT_USER,
            "dummy",
            "--" + OPT_CONNECTOR,
            "ranger",
            "--" + OPT_OUTPUT,
            "dummy",
            "--" + OPT_RANGER_DISABLE_TLS_VALIDATION);
    RangerClientHandle connectorHandle = (RangerClientHandle) connector.open(arguments);
    connectorHandle.rangerClient.findUsers(ImmutableMap.of());
  }
}
