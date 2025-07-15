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
package com.google.edwmigration.permissions.commands.buildcommand;

import static com.google.edwmigration.permissions.commands.buildcommand.AbstractRangerHiveToIamBindingMapper.ACCESS_POLICY_TYPE;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Service;
import com.google.edwmigration.permissions.utils.CollectionStreamProcessor;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class AbstractRangerHiveToIamBindingMapperTest {
  private static class TestAbstractRangerHiveToIamBindingMapper
      extends AbstractRangerHiveToIamBindingMapper {

    TestAbstractRangerHiveToIamBindingMapper(
        ImmutableList<Rule> rules,
        StreamProcessor<Table> tableReader,
        StreamProcessor<Principal> principalReader,
        StreamProcessor<Policy> rangerPolicyReader,
        StreamProcessor<Service> rangerServiceReader,
        String readIamRole,
        String writeIamRole) {
      super(
          rules,
          tableReader,
          principalReader,
          rangerPolicyReader,
          rangerServiceReader,
          readIamRole,
          writeIamRole);
    }

    @Override
    protected ImmutableList<IamBinding.Builder> mapTableToIamBinding(
        IamBinding.Builder builder, Table table) {
      return ImmutableList.of(
          builder.resourceType(ResourceType.BQ_TABLE).resourcePath(table.bqTable()));
    }
  }

  @Test
  public void policyMatchesTable_falseForNonHivePolicy() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table ignoredTable = Table.create("", "", "", "", "");

    Policy wrongServiceTypePolicy =
        Policy.builder().serviceType("wrong").id(0).service("").name("").build();

    assertFalse(mapper.policyMatchesTable(wrongServiceTypePolicy, ignoredTable));
  }

  @Test
  public void policyMatchesTable_falseForWrongPolicyType() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table ignoredTable = Table.create("", "", "", "", "");

    Policy wrongPolicyTypePolicy =
        Policy.builder().serviceType("hive").id(0).service("").name("").policyType(1).build();

    assertFalse(mapper.policyMatchesTable(wrongPolicyTypePolicy, ignoredTable));
  }

  @Test
  public void policyMatchesTable_falseForNullPolicyResources() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table ignoredTable = Table.create("", "", "", "", "");

    Policy nullResourcesPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .build();

    assertFalse(mapper.policyMatchesTable(nullResourcesPolicy, ignoredTable));
  }

  @Test
  public void policyMatchesTable_falseForEmptyPolicyResources() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table ignoredTable = Table.create("", "", "", "", "");

    Policy emptyResourcesPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(Collections.emptyMap())
            .build();

    assertFalse(mapper.policyMatchesTable(emptyResourcesPolicy, ignoredTable));
  }

  @Test
  public void policyMatchesTable_falseForNoDatabasesInPolicyResources() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table ignoredTable = Table.create("", "", "", "", "");

    Policy noDatabasesPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "notDatabase",
                    Policy.PolicyResource.builder().values(ImmutableList.of()).build()))
            .build();

    assertFalse(mapper.policyMatchesTable(noDatabasesPolicy, ignoredTable));
  }

  @Test
  public void policyMatchesTable_falseForMismatchedDatabase() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("project", "dataset", "table", "schema", "name");

    Policy mismatchedDbPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder()
                        .values(ImmutableList.of("wrong_database"))
                        .build(),
                    "table",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build()))
            .build();

    assertFalse(mapper.policyMatchesTable(mismatchedDbPolicy, table));
  }

  @Test
  public void policyMatchesTable_falseForMismatchedTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("project", "dataset", "table", "schema", "name");

    Policy mismatchedTablePolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build(),
                    "table",
                    Policy.PolicyResource.builder()
                        .values(ImmutableList.of("wrong_table"))
                        .build()))
            .build();

    assertFalse(mapper.policyMatchesTable(mismatchedTablePolicy, table));
  }

  @Test
  public void policyMatchesTable_trueForMatchingDatabaseAndTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("tab1", "db1", "", "", "");

    Policy matchingPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("db1")).build(),
                    "table",
                    Policy.PolicyResource.builder().values(ImmutableList.of("tab1")).build()))
            .build();

    assertTrue(mapper.policyMatchesTable(matchingPolicy, table));
  }

  @Test
  public void policyMatchesTable_trueForWildcardDatabaseAndMatchingTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("tab1", "db1", "", "", "");

    Policy matchingPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build(),
                    "table",
                    Policy.PolicyResource.builder().values(ImmutableList.of("tab1")).build()))
            .build();

    assertTrue(mapper.policyMatchesTable(matchingPolicy, table));
  }

  @Test
  public void policyMatchesTable_trueForWildcardDatabaseAndTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("tab1", "db1", "", "", "");

    Policy matchingPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build(),
                    "table",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build()))
            .build();

    assertTrue(mapper.policyMatchesTable(matchingPolicy, table));
  }

  @Test
  public void policyMatchesTable_trueForMatchingDatabaseAndNoTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("tab1", "db1", "", "", "");

    Policy matchingPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("db1")).build()))
            .build();

    assertTrue(mapper.policyMatchesTable(matchingPolicy, table));
  }

  @Test
  public void policyMatchesTable_trueForWildcardDatabaseAndNoTable() {
    TestAbstractRangerHiveToIamBindingMapper mapper =
        new TestAbstractRangerHiveToIamBindingMapper(
            ImmutableList.of(),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            new CollectionStreamProcessor<>(ImmutableList.of()),
            "readIamRole",
            "writeIamRole");
    Table table = Table.create("tab1", "db1", "", "", "");

    Policy matchingPolicy =
        Policy.builder()
            .serviceType("hive")
            .id(0)
            .service("")
            .name("")
            .policyType(ACCESS_POLICY_TYPE)
            .resources(
                ImmutableMap.of(
                    "database",
                    Policy.PolicyResource.builder().values(ImmutableList.of("*")).build()))
            .build();

    assertTrue(mapper.policyMatchesTable(matchingPolicy, table));
  }
}
