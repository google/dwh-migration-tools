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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerClient.ConnectionWrapper;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import java.io.IOException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public abstract class AbstractRangerTaskTest {

  @Mock protected ConnectionWrapper httpClient = mock(ConnectionWrapper.class);

  protected RangerClient rangerClient = new RangerClient(httpClient);

  protected RangerClientHandle handle = new RangerClientHandle(rangerClient, /* pageSize= */ 1000);

  protected MemoryByteSink sink = new MemoryByteSink();

  protected void mockSuccessfulResponseFromResource(String resource) throws IOException {
    when(httpClient.doGet(anyString(), Mockito.anyMap()))
        .thenReturn(RangerTestResources.getResourceAsString(resource));
  }
}
