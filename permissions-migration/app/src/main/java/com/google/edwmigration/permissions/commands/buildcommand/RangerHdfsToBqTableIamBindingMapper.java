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

import static com.google.edwmigration.permissions.commands.buildcommand.Roles.BQ_READ_IAM_ROLE;
import static com.google.edwmigration.permissions.commands.buildcommand.Roles.BQ_WRITE_IAM_ROLE;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Policy;
import com.google.edwmigration.permissions.models.ranger.RangerDumpFormat.Service;

public class RangerHdfsToBqTableIamBindingMapper extends AbstractRangerHdfsToIamBindingMapper {

  RangerHdfsToBqTableIamBindingMapper(
      ImmutableList<Rule> rules,
      StreamProcessor<Table> tableReader,
      StreamProcessor<Principal> principalReader,
      StreamProcessor<Policy> rangerPolicyReader,
      StreamProcessor<Service> rangerServiceReader) {
    super(
        rules,
        tableReader,
        principalReader,
        rangerPolicyReader,
        rangerServiceReader,
        BQ_READ_IAM_ROLE,
        BQ_WRITE_IAM_ROLE);
  }

  @Override
  protected ImmutableList<IamBinding.Builder> mapTableToIamBinding(
      IamBinding.Builder builder, Table table) {
    return ImmutableList.of(
        builder.resourceType(ResourceType.BQ_TABLE).resourcePath(table.bqTable()));
  }
}
