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
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.DumpServicesTask;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Service;
import java.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DumpServicesTaskTest extends AbstractTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RangerClient rangerClientMock;

  private static final ImmutableList<Service> TEST_SERVICES =
      ImmutableList.of(
          Service.create(
              /* id= */ 1L,
              /* guid= */ "4c2780ee-e244-4ac3-b21a-3660e6895980",
              /* isEnabled= */ true,
              /* createdBy= */ "Admin",
              /* updatedBy= */ "Admin",
              /* createTime= */ Instant.ofEpochSecond(1716795282),
              /* updateTime= */ Instant.ofEpochSecond(1716795282),
              /* version= */ 1L,
              /* type= */ "hdfs",
              /* name= */ "hadoop-dataproc",
              /* displayName= */ null,
              /* description= */ "Hadoop hdfs service",
              /* tagService= */ null,
              /* configs= */ ImmutableMap.of(
                  "hadoop.security.authentication", "Simple",
                  "hadoop.security.authorization", "No",
                  "fs.default.name", "hdfs://hadoop-m:8020",
                  "username", "admin",
                  // Ranger API redacts passwords, so there's no risk of fetching these.
                  "password", "*****"),
              /* policyVersion= */ 9L,
              /* policyUpdateTime= */ Instant.ofEpochSecond(1722016701),
              /* tagVersion= */ 1L,
              /* tagUpdateTime= */ Instant.ofEpochSecond(1722016702)),
          Service.create(
              /* id= */ 2L,
              /* guid= */ "1fe46519-233a-4780-be20-a7eb180f1ca6",
              /* isEnabled= */ true,
              /* createdBy= */ "Admin",
              /* updatedBy= */ "Admin",
              /* createTime= */ Instant.ofEpochSecond(1716795282),
              /* updateTime= */ Instant.ofEpochSecond(1716795282),
              /* version= */ 1L,
              /* type= */ "hive",
              /* name= */ "hive-dataproc",
              /* displayName= */ null,
              /* description= */ "Hadoop HIVE service",
              /* tagService= */ null,
              /* configs= */ ImmutableMap.of(
                  "jdbc.driverClassName", "org.apache.hive.jdbc.HiveDriver",
                  "jdbc.url", "jdbc:mysql://localhost",
                  "username", "admin",
                  // Ranger API redacts passwords, so there's no risk of fetching these.
                  "password", "*****"),
              /* policyVersion= */ 6L,
              /* policyUpdateTime= */ Instant.ofEpochSecond(1722016701),
              /* tagVersion= */ 1L,
              /* tagUpdateTime= */ Instant.ofEpochSecond(1722016702)));

  @Test
  public void doRun_success() throws Exception {
    when(rangerClientMock.findServices(anyMap())).thenReturn(TEST_SERVICES);
    DumpServicesTask task = new DumpServicesTask();
    RangerClientHandle handle = new RangerClientHandle(rangerClientMock, /* pageSize= */ 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(/* context= */ null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/services.success.jsonl");
    assertEquals(expected, actual);
  }
}
