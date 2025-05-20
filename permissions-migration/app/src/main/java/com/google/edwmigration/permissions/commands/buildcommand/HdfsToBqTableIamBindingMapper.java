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
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.commands.expand.StreamProcessor;
import com.google.edwmigration.permissions.models.HdfsPermission;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Principal;
import com.google.edwmigration.permissions.models.Rule;
import com.google.edwmigration.permissions.models.Table;

public class HdfsToBqTableIamBindingMapper extends AbstractHdfsToIamBindingMapper {

  HdfsToBqTableIamBindingMapper(
      ImmutableList<Rule> rules,
      StreamProcessor<Table> tableReader,
      StreamProcessor<Principal> principalReader,
      StreamProcessor<HdfsPermission> permissionReader) {
    super(
        rules, tableReader, principalReader, permissionReader, BQ_READ_IAM_ROLE, BQ_WRITE_IAM_ROLE);
  }

  @Override
  protected IamBinding.Builder mapTableToIamBinding(IamBinding.Builder builder, Table table) {
    return builder.resourceType(ResourceType.BQ_TABLE).resourcePath(table.bqTable());
  }

  @Override
  protected ImmutableMap<String, Object> mapTableToDefaultObjectMap(Table table) {
    return ImmutableMap.of("resourceType", ResourceType.BQ_TABLE, "resourcePath", table.bqTable());
  }
}
