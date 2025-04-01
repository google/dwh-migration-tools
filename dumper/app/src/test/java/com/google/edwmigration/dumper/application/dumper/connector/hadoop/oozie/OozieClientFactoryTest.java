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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import static java.nio.charset.StandardCharsets.*;
import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_AUTH;
import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_URL;
import static org.apache.oozie.cli.OozieCLI.OOZIE_RETRY_COUNT;
import static org.apache.oozie.cli.OozieCLI.WS_HEADER_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Base64;
import java.util.Properties;
import org.apache.oozie.cli.OozieCLIException;
import org.apache.oozie.client.AuthOozieClient.AuthType;
import org.apache.oozie.client.XOozieClient;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class OozieClientFactoryTest {
  @Test
  public void commonFlowWithoutUrlProvided() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(OozieClientFactory::getOozieUrlFromEnv).thenReturn("/some/url/path");

      // Act
      factory
          .when(() -> OozieClientFactory.createXOozieClient(any(), any(), any()))
          .thenCallRealMethod();
      OozieClientFactory.createXOozieClient(null, null, null);

      // Verify
      factory.verify(OozieClientFactory::getOozieUrlFromEnv);
      factory.verify(() -> OozieClientFactory.getAuthOption(null, null));
      factory.verify(
          () -> OozieClientFactory.addHeaders(any(XOozieClient.class), eq(null), eq(null)));
      factory.verify(() -> OozieClientFactory.setRetryCount(any(XOozieClient.class)));
    }
  }

  @Test
  public void commonFlowWithUrlProvided() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {

      // Act
      factory
          .when(() -> OozieClientFactory.createXOozieClient(any(), any(), any()))
          .thenCallRealMethod();
      OozieClientFactory.createXOozieClient("url", null, null);

      // Verify
      factory.verify(OozieClientFactory::getOozieUrlFromEnv, never());
      factory.verify(() -> OozieClientFactory.getAuthOption(null, null));
      factory.verify(
          () -> OozieClientFactory.addHeaders(any(XOozieClient.class), eq(null), eq(null)));
      factory.verify(() -> OozieClientFactory.setRetryCount(any(XOozieClient.class)));
    }
  }

  @Test
  public void getAuthOptionWithUsernameAndPassword() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      // Act
      factory.when(() -> OozieClientFactory.getAuthOption(any(), any())).thenCallRealMethod();
      String authOption = OozieClientFactory.getAuthOption("username", "secret");

      // Assert
      assertEquals(AuthType.BASIC.name(), authOption);
    }
  }

  @Test
  public void getAuthOptionFromEnv() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory
          .when(() -> OozieClientFactory.getEnvVariable(ENV_OOZIE_AUTH))
          .thenReturn("auth-option");

      // Act
      factory.when(() -> OozieClientFactory.getAuthOption(any(), any())).thenCallRealMethod();
      String authOption = OozieClientFactory.getAuthOption(null, null);

      // Assert
      assertEquals("auth-option", authOption);
    }
  }

  @Test
  public void getAuthOptionFromEnvNullCase() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(() -> OozieClientFactory.getEnvVariable(ENV_OOZIE_AUTH)).thenReturn(null);

      // Act
      factory.when(() -> OozieClientFactory.getAuthOption(any(), any())).thenCallRealMethod();
      String authOption = OozieClientFactory.getAuthOption(null, null);

      // Assert
      assertNull(authOption);
    }
  }

  @Test
  public void getAuthOptionFromEnvButBasic() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory
          .when(() -> OozieClientFactory.getEnvVariable(ENV_OOZIE_AUTH))
          .thenReturn(AuthType.BASIC.name());

      // Act
      factory.when(() -> OozieClientFactory.getAuthOption(any(), any())).thenCallRealMethod();

      OozieCLIException exception =
          assertThrows(OozieCLIException.class, () -> OozieClientFactory.getAuthOption(null, null));

      assertEquals(
          "BASIC authentication requires -user and -password to set!", exception.getMessage());
    }
  }

  @Test
  public void getAuthOptionWithUsernameAndNoPassword() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      // Act
      factory.when(() -> OozieClientFactory.getAuthOption(any(), any())).thenCallRealMethod();

      OozieCLIException exception =
          assertThrows(
              OozieCLIException.class, () -> OozieClientFactory.getAuthOption("username", null));

      assertEquals(
          "No password specified, it is required, if user is set!", exception.getMessage());
    }
  }

  @Test
  public void oozieUrlFromEnvProvided() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(() -> OozieClientFactory.getEnvVariable(ENV_OOZIE_URL)).thenReturn("some path");

      // Act
      factory.when(OozieClientFactory::getOozieUrlFromEnv).thenCallRealMethod();
      String url = OozieClientFactory.getOozieUrlFromEnv();

      // Assert
      assertEquals("some path", url);
    }
  }

  @Test
  public void oozieUrlFromEnvNotProvided() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(() -> OozieClientFactory.getEnvVariable(ENV_OOZIE_URL)).thenReturn(null);

      // Act
      factory.when(OozieClientFactory::getOozieUrlFromEnv).thenCallRealMethod();
      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, OozieClientFactory::getOozieUrlFromEnv);

      assertEquals(
          "Oozie URL is not available neither in command option nor in the environment",
          exception.getMessage());
    }
  }

  @Test
  public void retryWithValidEnvProperty() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(() -> OozieClientFactory.getEnvProperty(OOZIE_RETRY_COUNT)).thenReturn("6");
      XOozieClient oozieClient = mock(XOozieClient.class);

      // Act
      factory.when(() -> OozieClientFactory.setRetryCount(any())).thenCallRealMethod();
      OozieClientFactory.setRetryCount(oozieClient);

      // Verify
      verify(oozieClient).setRetryCount(6);
    }
  }

  @Test
  public void retryWithInvalidEnvProperty() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      factory.when(() -> OozieClientFactory.getEnvProperty(OOZIE_RETRY_COUNT)).thenReturn("abc");
      XOozieClient oozieClient = mock(XOozieClient.class);

      // Act
      factory.when(() -> OozieClientFactory.setRetryCount(any())).thenCallRealMethod();
      OozieClientFactory.setRetryCount(oozieClient);

      // Verify
      verify(oozieClient, never()).setRetryCount(anyInt());
    }
  }

  @Test
  public void addHeadersWithoutBasicAuth() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory.when(OozieClientFactory::getEnvProperties).thenReturn(new Properties());

      // Act
      factory.when(() -> OozieClientFactory.addHeaders(any(), any(), any())).thenCallRealMethod();
      OozieClientFactory.addHeaders(oozieClient, null, null);

      // Verify
      verify(oozieClient, never()).setHeader(anyString(), anyString());
    }
  }

  @Test
  public void addHeadersWithBasicAuth() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      factory.when(OozieClientFactory::getEnvProperties).thenReturn(new Properties());

      // Act
      factory.when(() -> OozieClientFactory.addHeaders(any(), any(), any())).thenCallRealMethod();
      OozieClientFactory.addHeaders(oozieClient, "user", "secret");

      // Verify
      String basicAuth =
          "Basic " + Base64.getEncoder().encodeToString("user:secret".getBytes(UTF_8));
      verify(oozieClient).setHeader("Authorization", basicAuth);
    }
  }

  @Test
  public void addHeadersFromEnv() throws Exception {
    try (MockedStatic<OozieClientFactory> factory = Mockito.mockStatic(OozieClientFactory.class)) {
      XOozieClient oozieClient = mock(XOozieClient.class);
      Properties envProps = new Properties();
      envProps.setProperty(WS_HEADER_PREFIX + "x", "val1");
      envProps.setProperty(WS_HEADER_PREFIX + "y", "val2");
      envProps.setProperty("another_prop", "value");
      factory.when(OozieClientFactory::getEnvProperties).thenReturn(envProps);

      // Act
      factory.when(() -> OozieClientFactory.addHeaders(any(), any(), any())).thenCallRealMethod();
      OozieClientFactory.addHeaders(oozieClient, null, null);

      // Verify
      verify(oozieClient).setHeader("x", "val1");
      verify(oozieClient).setHeader("y", "val2");
      verifyNoMoreInteractions(oozieClient);
    }
  }
}
