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

import static org.apache.oozie.cli.OozieCLI.ENV_OOZIE_URL;
import static org.apache.oozie.cli.OozieCLI.OOZIE_RETRY_COUNT;
import static org.apache.oozie.cli.OozieCLI.WS_HEADER_PREFIX;

import java.util.Map;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.cli.OozieCLIException;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;

/**
 * Factory does initialization of Oozie client in a similar but simplified way compare to {@link
 * OozieCLI#main(String[])}.
 */
public class OozieClientFactory {
  public static XOozieClient createXOozieClient() throws OozieCLIException {
    String oozieUrl = getOozieUrl();
    // String authOption = getAuthOption(commandLine);
    XOozieClient wc = new AuthOozieClient(oozieUrl, null);

    addHeaders(wc);
    setRetryCount(wc);
    return wc;
  }

  protected static String getOozieUrl() {
    String url = null; // commandLine.getOptionValue(OOZIE_OPTION);
    if (url == null) {
      url = System.getenv(ENV_OOZIE_URL);
      if (url == null) {
        throw new IllegalArgumentException(
            "Oozie URL is not available neither in command option or in the environment");
      }
    }
    return url;
  }

  protected static void addHeaders(OozieClient wc) {
    // String username = commandLine.getOptionValue(USERNAME);
    // String password = commandLine.getOptionValue(PASSWORD);
    // if (username != null && password != null) {
    //   String encoded = Base64.getEncoder().encodeToString((username + ':' + password).getBytes(
    //       StandardCharsets.UTF_8));
    //   wc.setHeader("Authorization", "Basic " + encoded);
    // }
    for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(WS_HEADER_PREFIX)) {
        String header = key.substring(WS_HEADER_PREFIX.length());
        System.out.println("Header added to Oozie client: " + header);
        wc.setHeader(header, (String) entry.getValue());
      }
    }
  }

  protected static void setRetryCount(OozieClient wc) {
    String retryCount = System.getProperty(OOZIE_RETRY_COUNT);
    if (retryCount != null && !retryCount.isEmpty()) {
      try {
        int retry = Integer.parseInt(retryCount.trim());
        wc.setRetryCount(retry);
      } catch (Exception ex) {
        System.err.println(
            "Unable to parse the retry settings. May be not an integer [" + retryCount + "]");
        ex.printStackTrace();
      }
    }
  }
}
