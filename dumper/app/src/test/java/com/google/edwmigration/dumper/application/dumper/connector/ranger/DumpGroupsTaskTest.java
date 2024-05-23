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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.DumpGroupsTask;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerInternalClient.Group;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import java.time.Instant;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DumpGroupsTaskTest extends AbstractTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RangerInternalClient rangerInternalClientMock;

  private static final List<Group> TEST_GROUPS =
      ImmutableList.of(
          Group.create(
              /* id= */ 1,
              /* createDate= */ Instant.ofEpochSecond(1716401255),
              /* updateDate= */ Instant.ofEpochSecond(1716487655),
              /* owner= */ "admin",
              /* updatedBy= */ "admin",
              /* name= */ "public",
              /* description= */ "public group",
              /* groupType= */ 0,
              /* groupSource= */ 0,
              /* credStoreId= */ null,
              /* isVisible= */ 1,
              /* otherAttributes= */ null,
              /* syncSource= */ null),
          Group.create(
              /* id= */ 21,
              /* createDate= */ Instant.ofEpochSecond(1716401255),
              /* updateDate= */ Instant.ofEpochSecond(1716487655),
              /* owner= */ "rangerusersync",
              /* updatedBy= */ "rangerusersync",
              /* name= */ "backup",
              /* description= */ "backup - add from Unix box",
              /* groupType= */ 1,
              /* groupSource= */ 1,
              /* credStoreId= */ null,
              /* isVisible= */ 1,
              /* otherAttributes= */ null,
              /* syncSource= */ null));

  @Test
  public void doRun_success() throws Exception {
    when(rangerInternalClientMock.findGroups(anyMap())).thenReturn(TEST_GROUPS);
    DumpGroupsTask task = new DumpGroupsTask();
    RangerClientHandle handle =
        new RangerClientHandle(/* rangerClient= */ null, rangerInternalClientMock, 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(/* context= */ null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/groups.success.jsonl");
    assertEquals(expected, actual);
  }
}
