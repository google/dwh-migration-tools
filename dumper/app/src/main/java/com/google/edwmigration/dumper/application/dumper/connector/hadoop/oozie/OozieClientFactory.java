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

import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_AUTH;
import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_URL;
import static org.apache.oozie.cli.OozieCLI.OOZIE_RETRY_COUNT;
import static org.apache.oozie.cli.OozieCLI.WS_HEADER_PREFIX;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.cli.OozieCLIException;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.AuthOozieClient.AuthType;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory does initialization of Oozie client in a similar but simplified way compare to {@link
 * OozieCLI#main(String[])}.
 */
public class OozieClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(OozieClientFactory.class);

  public static XOozieClient createXOozieClient(String oozieUrl, String user, String password)
      throws OozieCLIException {
    oozieUrl = oozieUrl != null ? oozieUrl : getOozieUrlFromEnv();
    String authOption = getAuthOption(user, password);
    XOozieClient oozieClient = new AuthOozieClient(oozieUrl, authOption);

    // mimic oozie CLI configuration
    addHeaders(oozieClient, user, password);
    setRetryCount(oozieClient);
    return oozieClient;
  }

  protected static String getAuthOption(String user, String password) throws OozieCLIException {
    if (user != null) {
      if (password == null) {
        throw new OozieCLIException("No password specified, it is required, if user is set!");
      }
      return AuthType.BASIC.name();
    }
    String authOpt = getEnvVariable(ENV_OOZIE_AUTH);
    logger.debug("Auth type for Oozie client: {} base on {} env var", authOpt, ENV_OOZIE_AUTH);
    if (AuthType.BASIC.name().equalsIgnoreCase(authOpt)) {
      throw new OozieCLIException("BASIC authentication requires -user and -password to set!");
    }
    return authOpt;
  }

  protected static String getOozieUrlFromEnv() {
    // oozie CLI expect this env variable, so we use it as a fallback
    String url = getEnvVariable(ENV_OOZIE_URL);
    if (url == null) {
      throw new IllegalArgumentException(
          "Oozie URL is not available neither in command option nor in the environment");
    }
    logger.debug("Oozie URL: {} base on {} env var", url, ENV_OOZIE_URL);
    return url;
  }

  protected static void addHeaders(OozieClient wc, String user, String password) {
    if (user != null && password != null) {
      String encoded =
          Base64.getEncoder()
              .encodeToString((user + ':' + password).getBytes(StandardCharsets.UTF_8));
      wc.setHeader("Authorization", "Basic " + encoded);
    }
    for (Map.Entry<Object, Object> entry : getEnvProperties().entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(WS_HEADER_PREFIX)) {
        String header = key.substring(WS_HEADER_PREFIX.length());
        logger.debug("Header added to Oozie client: {}", header);
        wc.setHeader(header, (String) entry.getValue());
      }
    }
  }

  protected static void setRetryCount(OozieClient oozieClient) {
    String retryCount = getEnvProperty(OOZIE_RETRY_COUNT);
    if (retryCount != null && !retryCount.isEmpty()) {
      try {
        int retry = Integer.parseInt(retryCount.trim());
        oozieClient.setRetryCount(retry);
      } catch (NumberFormatException ex) {
        logger.error(
            "Unable to parse the retry settings. May be not an integer [{}]", retryCount, ex);
      }
    }
  }

  /** It's just a wrapper for {@code System.getenv(property)} Extracted to mock in unit-tests. */
  protected static String getEnvVariable(String name) {
    return System.getenv(name);
  }

  /**
   * It's just a wrapper for {@code System.getProperty(property)} Extracted to mock in unit-tests.
   */
  protected static String getEnvProperty(String property) {
    return System.getProperty(property);
  }

  /** It's just a wrapper for {@code System.getProperties()} Extracted to mock in unit-tests. */
  protected static Properties getEnvProperties() {
    return System.getProperties();
  }
}
