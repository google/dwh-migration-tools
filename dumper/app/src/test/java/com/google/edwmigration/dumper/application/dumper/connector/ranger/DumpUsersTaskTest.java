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
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.DumpUsersTask;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.User;
import java.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DumpUsersTaskTest extends AbstractTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RangerClient rangerClientMock;

  private static final ImmutableList<User> TEST_SERVICES =
      ImmutableList.of(
          User.create(
              /* id= */ 1,
              /* createDate= */ Instant.ofEpochSecond(1716401255),
              /* updateDate= */ Instant.ofEpochSecond(1716487655),
              /* owner= */ null,
              /* updatedBy= */ "admin",
              /* name= */ "admin",
              /* firstName= */ "Admin",
              /* lastName= */ "",
              /* emailAddress= */ null,
              /* credStoreId= */ null,
              /* description= */ "Administrator",
              /* groupIdList= */ ImmutableList.of(),
              /* groupNameList= */ ImmutableList.of(),
              /* status= */ 1,
              /* isVisible= */ 1,
              /* userSource= */ 0,
              /* userRoleList= */ ImmutableList.of("ROLE_SYS_ADMIN"),
              /* otherAttributes= */ null,
              /* syncSource= */ null),
          User.create(
              /* id= */ 2,
              /* createDate= */ Instant.ofEpochSecond(1716401215),
              /* updateDate= */ Instant.ofEpochSecond(1716487615),
              /* owner= */ null,
              /* updatedBy= */ "admin",
              /* name= */ "rangerusersync",
              /* firstName= */ "rangerusersync",
              /* lastName= */ "",
              /* emailAddress= */ null,
              /* credStoreId= */ null,
              /* description= */ "rangerusersync",
              /* groupIdList= */ ImmutableList.of(),
              /* groupNameList= */ ImmutableList.of(),
              /* status= */ 1,
              /* isVisible= */ 1,
              /* userSource= */ 0,
              /* userRoleList= */ ImmutableList.of("ROLE_SYS_ADMIN"),
              /* otherAttributes= */ null,
              /* syncSource= */ null));

  @Test
  public void doRun_success() throws Exception {
    when(rangerClientMock.findUsers(anyMap())).thenReturn(TEST_SERVICES);
    DumpUsersTask task = new DumpUsersTask();
    RangerClientHandle handle = new RangerClientHandle(rangerClientMock, /* pageSize= */ 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(/* context= */ null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/users.success.jsonl");
    assertEquals(expected, actual);
  }
}
