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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClouderaManagerLoginHelperTest {

  private final URI uri = URI.create("http://localhost");
  @Mock private CloseableHttpClient httpClient;
  @Mock private CloseableHttpResponse login;
  @Mock private CloseableHttpResponse home;

  @Test
  public void login_success() throws Exception {
    String name = "login-name";
    String password = "secret";

    when(httpClient.execute(any(HttpPost.class))).thenReturn(login);
    when(httpClient.execute(any(HttpGet.class))).thenReturn(home);
    when(home.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "Ok"));

    ClouderaManagerLoginHelper.login(uri, httpClient, name, password);

    verify(httpClient, times(2))
        .execute(
            argThat(
                request -> {
                  if ("POST".equals(request.getMethod())) {
                    // assert login
                    assertEquals(HttpPost.class, request.getClass());
                    HttpPost post = (HttpPost) request;

                    assertEquals(uri + "/j_spring_security_check", request.getURI().toString());
                    HttpEntity body = post.getEntity();
                    assertEquals(
                        "application/x-www-form-urlencoded", body.getContentType().getValue());
                    assertTrue(URLEncodedUtils.isEncoded(body));
                    try {
                      List<NameValuePair> form = URLEncodedUtils.parse(body);

                      assertEquals(
                          ImmutableList.of(
                              new BasicNameValuePair("j_username", name),
                              new BasicNameValuePair("j_password", password)),
                          form);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  if ("GET".equals(request.getMethod())) {
                    // assert home page
                    assertEquals(HttpGet.class, request.getClass());
                    HttpGet get = (HttpGet) request;

                    assertEquals(uri + "/cmf/home", request.getURI().toString());
                  }
                  return true;
                }));

    verify(login).close();
    verify(home).close();
    verify(httpClient, never()).close();
  }

  @Test
  public void login_wrong_credentials() throws Exception {
    String name = "login-name";
    String password = "secret";

    when(httpClient.execute(any(HttpPost.class))).thenReturn(login);
    when(httpClient.execute(any(HttpGet.class))).thenReturn(home);
    when(home.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 401, "Ok"));

    assertThrows(
        MetadataDumperUsageException.class,
        () -> ClouderaManagerLoginHelper.login(uri, httpClient, name, password));

    verify(httpClient, times(2))
        .execute(
            argThat(
                request -> {
                  if ("POST".equals(request.getMethod())) {
                    // assert login
                    assertEquals(HttpPost.class, request.getClass());
                    HttpPost post = (HttpPost) request;

                    assertEquals(uri + "/j_spring_security_check", request.getURI().toString());
                    HttpEntity body = post.getEntity();
                    assertEquals(
                        "application/x-www-form-urlencoded", body.getContentType().getValue());
                    assertTrue(URLEncodedUtils.isEncoded(body));
                    try {
                      List<NameValuePair> form = URLEncodedUtils.parse(body);

                      assertEquals(
                          ImmutableList.of(
                              new BasicNameValuePair("j_username", name),
                              new BasicNameValuePair("j_password", password)),
                          form);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  if ("GET".equals(request.getMethod())) {
                    // assert home page
                    assertEquals(HttpGet.class, request.getClass());
                    HttpGet get = (HttpGet) request;

                    assertEquals(uri + "/cmf/home", request.getURI().toString());
                  }
                  return true;
                }));

    verify(login).close();
    verify(home).close();
    verify(httpClient, never()).close();
  }

  @Test
  public void login_connection_error_or_timeout() throws Exception {
    String name = "login-name";
    String password = "secret";

    when(httpClient.execute(any(HttpPost.class))).thenThrow(new ConnectException());

    assertThrows(
        ConnectException.class,
        () -> ClouderaManagerLoginHelper.login(uri, httpClient, name, password));

    verify(httpClient, times(1))
        .execute(
            argThat(
                request -> {
                  if ("POST".equals(request.getMethod())) {
                    // assert login
                    assertEquals(HttpPost.class, request.getClass());
                    HttpPost post = (HttpPost) request;

                    assertEquals(uri + "/j_spring_security_check", request.getURI().toString());
                    HttpEntity body = post.getEntity();
                    assertEquals(
                        "application/x-www-form-urlencoded", body.getContentType().getValue());
                    assertTrue(URLEncodedUtils.isEncoded(body));
                    try {
                      List<NameValuePair> form = URLEncodedUtils.parse(body);

                      assertEquals(
                          ImmutableList.of(
                              new BasicNameValuePair("j_username", name),
                              new BasicNameValuePair("j_password", password)),
                          form);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return true;
                }));

    verify(login, never()).close();
    verify(httpClient, never()).close();
  }
}
