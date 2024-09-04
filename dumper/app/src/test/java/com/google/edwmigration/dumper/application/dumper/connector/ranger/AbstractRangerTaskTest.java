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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public abstract class AbstractRangerTaskTest extends AbstractTaskTest {

  @Mock protected CloseableHttpClient httpClient = mock(CloseableHttpClient.class);

  protected RangerClient rangerClient =
      new RangerClient(httpClient, URI.create("http://localhost/ranger"), "user", "password");

  protected RangerClientHandle handle = new RangerClientHandle(rangerClient, /* pageSize= */ 1000);

  protected MemoryByteSink sink = new MemoryByteSink();

  private static class TestCloseableHttpResponse extends BasicHttpResponse
      implements CloseableHttpResponse {

    public TestCloseableHttpResponse(StatusLine statusline) {
      super(statusline);
    }

    @Override
    public void close() throws IOException {}
  }

  protected void mockSuccessfulResponseFromResource(String resource) throws IOException {
    StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "");
    TestCloseableHttpResponse response = new TestCloseableHttpResponse(statusLine);
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(RangerTestResources.getResourceAsInputStream(resource));
    response.setEntity(entity);
    when(httpClient.execute(Mockito.any(HttpGet.class))).thenReturn(response);
  }
}
