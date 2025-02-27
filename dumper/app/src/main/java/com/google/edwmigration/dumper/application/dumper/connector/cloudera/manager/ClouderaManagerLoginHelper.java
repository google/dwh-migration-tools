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

import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The helper does login into Cloudera Manager UI for {@code CloseableHttpClient} */
public final class ClouderaManagerLoginHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ClouderaManagerLoginHelper.class);

  /**
   * The helper does login into Cloudera Manager UI. The session is stored in a cookie in {@code
   * httpClient}, so all the http requests with the same {@code httpClient} will be authorised.
   *
   * @param baseURI base url for Cloudera Manager UI, typically {@code https://master-node:7183}
   * @param httpClient http client to authorise
   * @param username login
   * @param password password
   * @throws Exception throws {@code Exception} for any unsuccessful login
   */
  public static void login(
      URI baseURI, CloseableHttpClient httpClient, String username, String password)
      throws Exception {
    HttpPost post = new HttpPost(baseURI + "/j_spring_security_check");
    List<NameValuePair> urlParameters = new ArrayList<>();
    urlParameters.add(new BasicNameValuePair("j_username", username));
    urlParameters.add(new BasicNameValuePair("j_password", password));

    post.setEntity(new UrlEncodedFormEntity(urlParameters));

    try (CloseableHttpResponse login = httpClient.execute(post)) {
      // verify success login by home page access
      try (CloseableHttpResponse home = httpClient.execute(new HttpGet(baseURI + "/cmf/home"))) {
        if (HttpStatus.SC_OK != home.getStatusLine().getStatusCode()) {
          LOG.error(
              "Login to Cloudera Manager wasn't successful. "
                  + "The response code for /cmf/home page is [{}] and response: {}",
              home.getStatusLine().getStatusCode(),
              home);

          throw new MetadataDumperUsageException("Login wasn't successful: " + login);
        }
      }
    }
    LOG.info("Success login into Cloudera Manager.");
  }
}
