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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy.PolicyItem;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy.PolicyItemAccess;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy.PolicyResource;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy.RowFilterPolicyItem;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.RangerDumpFormat.Policy.RowFilterPolicyItem.PolicyItemRowFilterInfo;
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

  private static final ImmutableList<Policy> TEST_POLICIES =
      ImmutableList.of(
          // An access policy.
          Policy.create(
              /* id= */ 1L,
              /* guid= */ "63bbd745-5328-4afc-b06e-cfa9fa60514b",
              /* isEnabled= */ true,
              /* createdBy= */ null,
              /* updatedBy= */ null,
              /* createDate= */ null,
              /* updateDate= */ null,
              /* version= */ 6L,
              /* service= */ "hive-dataproc",
              /* name= */ "all - hiveservice",
              /* policyType= */ 0,
              /* policyPriority= */ 0,
              /* description= */ "Policy for all - hiveservice",
              /* resourceSignature= */ null,
              /* isAuditEnabled= */ true,
              /* resources= */ ImmutableMap.of(
                  "database",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("*"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false),
                  "table",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("*"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false),
                  "column",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("*"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false)),
              /* additionalResources= */ ImmutableList.of(),
              /* conditions= */ ImmutableList.of(),
              /* policyItems= */ ImmutableList.of(
                  PolicyItem.create(
                      /* accesses= */ ImmutableList.of(
                          PolicyItemAccess.create("select", true),
                          PolicyItemAccess.create("update", true),
                          PolicyItemAccess.create("create", true),
                          PolicyItemAccess.create("drop", true),
                          PolicyItemAccess.create("alter", true),
                          PolicyItemAccess.create("index", true),
                          PolicyItemAccess.create("lock", true),
                          PolicyItemAccess.create("all", true),
                          PolicyItemAccess.create("read", true),
                          PolicyItemAccess.create("write", true),
                          PolicyItemAccess.create("repladmin", true),
                          PolicyItemAccess.create("serviceadmin", true),
                          PolicyItemAccess.create("tempudfadmin", true),
                          PolicyItemAccess.create("refresh", true)),
                      /* users= */ ImmutableList.of("admin", "hive"),
                      /* groups= */ ImmutableList.of(),
                      /* roles= */ ImmutableList.of(),
                      /* conditions= */ ImmutableList.of(),
                      /* delegateAdmin= */ false)),
              /* denyPolicyItems= */ ImmutableList.of(),
              /* allowExceptions= */ ImmutableList.of(),
              /* denyExceptions= */ ImmutableList.of(),
              /* dataMaskPolicyItems= */ ImmutableList.of(),
              /* rowFilterPolicyItems= */ ImmutableList.of(),
              /* serviceType= */ "1",
              /* options= */ ImmutableMap.of(),
              /* validitySchedule= */ ImmutableList.of(),
              /* policyLabels= */ ImmutableList.of(),
              /* zoneName= */ "",
              /* isDenyAllElse= */ false),

          // A row level filter policy.
          Policy.create(
              /* id= */ 10L,
              /* guid= */ "e37ab2af-8a27-47dc-b7f8-896c8576a86b",
              /* isEnabled= */ true,
              /* createdBy= */ null,
              /* updatedBy= */ null,
              /* createDate= */ null,
              /* updateDate= */ null,
              /* version= */ 4L,
              /* service= */ "hive-dataproc",
              /* name= */ "filter-col1",
              /* policyType= */ 2,
              /* policyPriority= */ 0,
              /* description= */ "Filter col1 values",
              /* resourceSignature=*/ null,
              /* isAuditEnabled= */ true,
              /* resources= */ ImmutableMap.of(
                  "database",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("default"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false),
                  "table",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("table"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false)),
              /* additionalResources= */ ImmutableList.of(),
              /* conditions= */ ImmutableList.of(),
              /* policyItems= */ ImmutableList.of(),
              /* denyPolicyItems= */ ImmutableList.of(),
              /* allowExceptions= */ ImmutableList.of(),
              /* denyExceptions= */ ImmutableList.of(),
              /* dataMaskPolicyItems= */ ImmutableList.of(),
              /* rowFilterPolicyItems= */ ImmutableList.of(
                  RowFilterPolicyItem.create(
                      /* accesses= */ ImmutableList.of(PolicyItemAccess.create("select", true)),
                      /* users= */ ImmutableList.of("admin", "hive"),
                      /* groups= */ ImmutableList.of(),
                      /* roles= */ ImmutableList.of(),
                      /* conditions= */ ImmutableList.of(),
                      /* delegateAdmin= */ false,
                      /* rowFilterInfo= */ PolicyItemRowFilterInfo.create(
                          /* filterExpr= */ "col1 = \"value\""))),
              /* serviceType= */ "1",
              /* options= */ ImmutableMap.of(),
              /* validitySchedule= */ ImmutableList.of(),
              /* policyLabels= */ ImmutableList.of(),
              /* zoneName= */ "",
              /* isDenyAllElse= */ false),

          // A data masking policy.
          Policy.create(
              /* id= */ 11L,
              /* guid= */ "76d66c4e-308a-4142-9285-d4da2bd15e92",
              /* isEnabled= */ true,
              /* createdBy= */ null,
              /* updatedBy= */ null,
              /* createDate= */ null,
              /* updateDate= */ null,
              /* version= */ 2L,
              /* service= */ "hive-dataproc",
              /* name= */ "mask-col2",
              /* policyType= */ 1,
              /* policyPriority= */ 0,
              /* description= */ "Mask col2 values",
              /* resourceSignature=*/ null,
              /* isAuditEnabled= */ true,
              /* resources= */ ImmutableMap.of(
                  "database",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("default"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false),
                  "table",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("table"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false),
                  "column",
                  PolicyResource.create(
                      /* values= */ ImmutableList.of("col2"),
                      /* isExcludes= */ false,
                      /* isRecursive= */ false)),
              /* additionalResources= */ ImmutableList.of(),
              /* conditions= */ ImmutableList.of(),
              /* policyItems= */ ImmutableList.of(),
              /* denyPolicyItems= */ ImmutableList.of(),
              /* allowExceptions= */ ImmutableList.of(),
              /* denyExceptions= */ ImmutableList.of(),
              /* dataMaskPolicyItems= */ ImmutableList.of(),
              /* rowFilterPolicyItems= */ ImmutableList.of(
                  RowFilterPolicyItem.create(
                      /* accesses= */ ImmutableList.of(PolicyItemAccess.create("select", true)),
                      /* users= */ ImmutableList.of("admin", "hive"),
                      /* groups= */ ImmutableList.of(),
                      /* roles= */ ImmutableList.of(),
                      /* conditions= */ ImmutableList.of(),
                      /* delegateAdmin= */ false,
                      /* rowFilterInfo= */ PolicyItemRowFilterInfo.create(
                          /* filterExpr= */ "col1 = \"value\""))),
              /* serviceType= */ "1",
              /* options= */ ImmutableMap.of(),
              /* validitySchedule= */ ImmutableList.of(),
              /* policyLabels= */ ImmutableList.of(),
              /* zoneName= */ "",
              /* isDenyAllElse= */ false));

  @Test
  public void doRun_success() throws Exception {
    when(rangerClientMock.findPolicies(anyMap())).thenReturn(TEST_POLICIES);
    DumpPoliciesTask task = new DumpPoliciesTask();
    RangerClientHandle handle = new RangerClientHandle(rangerClientMock, /* pageSize= */ 1000);
    MemoryByteSink sink = new MemoryByteSink();

    task.doRun(/* context= */ null, sink, handle);

    String actual = sink.openStream().toString();
    String expected = RangerTestResources.getResourceAsString("ranger/policies.success.jsonl");
    assertEquals(expected, actual);
  }
}
