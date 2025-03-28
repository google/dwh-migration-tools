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

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyItemAccess;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy.PolicyResource;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Service;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRangerHdfsToIamBindingMapper extends AbstractRangerPermissionMapper {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRangerHdfsToIamBindingMapper.class);

  private static final String RANGER_HDFS_SERVICE = "hdfs";

  private static final String RANGER_PATH_RESOURCE = "path";

  private static final ImmutableSet<String> RANGER_WRITE_ACCESSES = ImmutableSet.of("write");

  private static final ImmutableSet<String> RANGER_READ_ACCESSES = ImmutableSet.of("read");

  private final String readIamRole;

  private final String writeIamRole;

  AbstractRangerHdfsToIamBindingMapper(
      ImmutableList<Rule> rules,
      StreamProcessor<Table> tableReader,
      StreamProcessor<Principal> principalReader,
      StreamProcessor<Policy> rangerPolicyReader,
      StreamProcessor<Service> rangerServiceReader,
      String readIamRole,
      String writeIamRole) {
    super(
        "HDFS",
        rules,
        tableReader,
        principalReader,
        rangerPolicyReader,
        rangerServiceReader,
        RANGER_HDFS_SERVICE);
    this.readIamRole = readIamRole;
    this.writeIamRole = writeIamRole;
  }

  @Override
  protected boolean policyMatchesTable(Policy policy, Table table) {
    PolicyResource pathResource = policy.resources().get(RANGER_PATH_RESOURCE);
    if (pathResource == null) {
      throw new IllegalStateException(
          "Ranger HDFS policy " + policy.name() + " has no path resource");
    }
    if (pathResource.values().stream()
        .noneMatch(
            path ->
                policyHdfsPathMatchesTable(
                    path, toBooleanOrFalse(pathResource.isRecursive()), table))) {
      LOG.debug(
          "Table '{}'.'{}' does not match HDFS policy '{}' path resource",
          table.schemaName(),
          table.fullName(),
          policy.name());
      return false;
    }
    return true;
  }

  @Override
  protected Optional<String> getRoleForAccesses(List<PolicyItemAccess> accesses) {
    ImmutableSet<String> accessesSet =
        accesses.stream()
            .filter(PolicyItemAccess::isAllowed)
            .map(PolicyItemAccess::type)
            .collect(toImmutableSet());
    if (!intersection(accessesSet, RANGER_WRITE_ACCESSES).isEmpty()) {
      return Optional.of(writeIamRole);
    }
    if (!intersection(accessesSet, RANGER_READ_ACCESSES).isEmpty()) {
      return Optional.of(readIamRole);
    }
    return Optional.empty();
  }

  private boolean policyHdfsPathMatchesTable(String path, boolean recursive, Table table) {
    String tablePath = URI.create(table.hdfsPath()).getPath();
    return new RangerPathPattern(path, recursive).compile().matches(tablePath);
  }

  private boolean toBooleanOrFalse(Boolean value) {
    return value != null && value;
  }
}
