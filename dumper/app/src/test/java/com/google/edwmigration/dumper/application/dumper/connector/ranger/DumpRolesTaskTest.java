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
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.DumpRolesTask;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import java.util.List;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerRole.RoleMember;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DumpRolesTaskTest extends AbstractTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RangerInternalClient rangerInternalClientMock;

  private static final List<RangerRole> TEST_ROLES =
      ImmutableList.of(
          new RangerRole(
              /* name= */ "auditor",
              /* description= */ "Auditor",
              /* options= */ ImmutableMap.of(),
              /* users= */ ImmutableList.of(
                  new RoleMember(/* name= */ "aleofreddi", /* isAdmin= */ true)),
              /* groups= */ ImmutableList.of(),
              /* roles= */ ImmutableList.of()),
          new RangerRole(
              /* name= */ "dba",
              /* description= */ "Database administrator",
              /* options= */ ImmutableMap.of(),
              /* users= */ ImmutableList.of(
                  new RoleMember(/* name= */ "aleofreddi", /* isAdmin= */ true)),
              /* groups= */ ImmutableList.of(),
              /* roles= */ ImmutableList.of()));

  @Test
  public void doRun_success() throws Exception {
    when(rangerInternalClientMock.findRoles(anyMap())).thenReturn(TEST_ROLES);
    DumpRolesTask task = new DumpRolesTask();
    RangerClientHandle handle =
        new RangerClientHandle(/* rangerClient= */ null, rangerInternalClientMock, 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(/* context= */ null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/roles.success.jsonl");
    assertEquals(expected, actual);
  }
}
