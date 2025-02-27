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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.dto.ApiYARNApplicationDTO;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractClouderaYarnApplicationTaskTest {
  private static WireMockServer server;
  private MockedYarnApplicationTask task;
  private ClouderaManagerHandle handle;
  private List<ApiYARNApplicationDTO> loadResponse;

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
  public void setUp() throws Exception {
    server.resetAll();
    URI uri = URI.create(server.baseUrl() + "/api/vTest/");
    handle = new ClouderaManagerHandle(uri, HttpClients.createDefault());

    task = new MockedYarnApplicationTask();
    loadResponse = new ArrayList<>();
  }

  @Test
  public void paginatedLoad_twoPages_callApiThreeTimes() {
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("limit", matching("2"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\": [{\"applicationId\":\"app1\"}]}");

    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\": [{\"applicationId\":\"app2\"}]}");

    queryParams.put("offset", matching("2"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\": []}");

    task.mockedLoad("test-cluster");

    Assert.assertEquals(loadResponse.size(), 2);
    Assert.assertEquals(loadResponse.get(0).getApplicationId(), "app1");
    Assert.assertEquals(loadResponse.get(1).getApplicationId(), "app2");
    server.verify(
        3,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
  }

  @Test
  public void paginatedLoad_singlePage_callApiTwice() {
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("limit", matching("2"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI(
        "test-cluster", queryParams, "{\"applications\": [{\"applicationId\":\"app1\"}]}");

    queryParams.put("offset", matching("1"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\": []}");

    task.mockedLoad("test-cluster");

    Assert.assertEquals(loadResponse.size(), 1);
    Assert.assertEquals(loadResponse.get(0).getApplicationId(), "app1");
    server.verify(
        2,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
  }

  @Test
  public void paginatedLoad_noPages_callApiOnce() {
    Map<String, StringValuePattern> queryParams = new HashMap<>();
    queryParams.put("limit", matching("2"));
    queryParams.put("offset", matching("0"));
    stubYARNApplicationsAPI("test-cluster", queryParams, "{\"applications\": []}");

    task.mockedLoad("test-cluster");

    Assert.assertEquals(loadResponse.size(), 0);
    server.verify(
        1,
        getRequestedFor(
            urlPathMatching("/api/vTest/clusters/test-cluster/services/yarn/yarnApplications.*")));
  }

  private void stubYARNApplicationsAPI(
      String clusterName, Map<String, StringValuePattern> queryParams, String responseContent) {
    stubYARNApplicationsAPI(clusterName, queryParams, responseContent, HttpStatus.SC_OK);
  }

  private void stubYARNApplicationsAPI(
      String clusterName,
      Map<String, StringValuePattern> queryParams,
      String responseContent,
      int statusCode) {
    server.stubFor(
        get(urlPathMatching(
                String.format(
                    "/api/vTest/clusters/%s/services/yarn/yarnApplications.*", clusterName)))
            .withQueryParams(queryParams)
            .willReturn(okJson(responseContent).withStatus(statusCode)));
  }

  private class MockedYarnApplicationTask extends AbstractClouderaYarnApplicationTask {
    public void mockedLoad(String clusterName) {
      PaginatedClouderaYarnApplicationsLoader loader =
          new PaginatedClouderaYarnApplicationsLoader(handle, 2);
      loader.load(clusterName, loadedApps -> loadResponse.addAll(loadedApps));
    }

    public MockedYarnApplicationTask() {
      super("", 30);
    }

    @Override
    protected void doRun(
        TaskRunContext context, @Nonnull ByteSink sink, @Nonnull ClouderaManagerHandle handle) {
      throw new UnsupportedOperationException("Test implementation");
    }
  }
}
