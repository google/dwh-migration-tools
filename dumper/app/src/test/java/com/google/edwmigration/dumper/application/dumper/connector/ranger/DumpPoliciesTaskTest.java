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
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.DumpPoliciesTask;
import com.google.edwmigration.dumper.application.dumper.connector.ranger.RangerConnector.RangerClientHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTaskTest;
import java.util.List;
import org.apache.ranger.RangerClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DumpPoliciesTaskTest extends AbstractTaskTest {

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private RangerClient rangerClientMock;

  private static final List<RangerPolicy> TEST_POLICIES =
      ImmutableList.of(
          new RangerPolicy(
              /* service= */ "hive-dataproc",
              /* name= */ "all - hiveservice",
              /* policyType= */ 0,
              /* policyPriority= */ 0,
              /* description= */ "Policy for all - hiveservice",
              /* resources= */ ImmutableMap.of(
              "database",
              new RangerPolicyResource(
                  /* values= */ "*", /* isExcludes= */ false, /* isRecursive= */ false),
              "table",
              new RangerPolicyResource(
                  /* values= */ "*", /* isExcludes= */ false, /* isRecursive= */ false),
              "column",
              new RangerPolicyResource(
                  /* values= */ "*", /* isExcludes= */ false, /* isRecursive= */ false)),
              /* policyItems= */ ImmutableList.of(
              new RangerPolicyItem(
                  /* accessTypes= */ ImmutableList.of(
                  new RangerPolicy.RangerPolicyItemAccess("select", true),
                  new RangerPolicy.RangerPolicyItemAccess("update", true),
                  new RangerPolicy.RangerPolicyItemAccess("create", true),
                  new RangerPolicy.RangerPolicyItemAccess("drop", true),
                  new RangerPolicy.RangerPolicyItemAccess("alter", true),
                  new RangerPolicy.RangerPolicyItemAccess("index", true),
                  new RangerPolicy.RangerPolicyItemAccess("lock", true),
                  new RangerPolicy.RangerPolicyItemAccess("all", true),
                  new RangerPolicy.RangerPolicyItemAccess("read", true),
                  new RangerPolicy.RangerPolicyItemAccess("write", true),
                  new RangerPolicy.RangerPolicyItemAccess("repladmin", true),
                  new RangerPolicy.RangerPolicyItemAccess("serviceadmin", true),
                  new RangerPolicy.RangerPolicyItemAccess("tempudfadmin", true),
                  new RangerPolicy.RangerPolicyItemAccess("refresh", true)),
                  /* users= */ ImmutableList.of("admin", "hive"),
                  /* groups= */ ImmutableList.of(),
                  /* roles= */ ImmutableList.of(),
                  /* conditions= */ ImmutableList.of(),
                  /* delegateAdmin= */ false)),
              /* resourceSignature=*/ null,
              /* options= */ ImmutableMap.of(),
              /* validitySchedule= */ ImmutableList.of(),
              /* policyLabels= */ ImmutableList.of(),
              /* zoneName= */ "",
              /* conditions= */ ImmutableList.of(),
              /* isDenyAllElse= */ false),
          new RangerPolicy(
              /* service= */ "hive-dataproc",
              /* name= */ "all - database, table, column",
              /* policyType= */ 0,
              /* policyPriority= */ 0,
              /* description= */ "Policy for all - database, table, column",
              /* resources= */ ImmutableMap.of(),
              /* policyItems= */ ImmutableList.of(),
              /* resourceSignature=*/ null,
              /* options= */ ImmutableMap.of(),
              /* validitySchedule= */ ImmutableList.of(),
              /* policyLabels= */ ImmutableList.of(),
              /* zoneName= */ "",
              /* conditions= */ ImmutableList.of(),
              /* isDenyAllElse= */ false));

  @Test
  public void doRun_success() throws Exception {
    when(rangerClientMock.findPolicies(anyMap())).thenReturn(TEST_POLICIES);
    DumpPoliciesTask task = new DumpPoliciesTask();
    RangerClientHandle handle = new RangerClientHandle(rangerClientMock, 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/policies.success.jsonl");
    assertEquals(expected, actual);
  }
}
