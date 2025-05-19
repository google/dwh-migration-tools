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
package com.google.edwmigration.permissions.commands.apply;

import com.google.cloud.Identity;
import com.google.cloud.Role;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.ExtraPermissions;
import com.google.edwmigration.permissions.IamClient;
import com.google.edwmigration.permissions.IamClient.GcpResource;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Permissions;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Applies a list of permissions to GCP resources. */
public class PermissionsApplier {

  private final ImmutableMap<ResourceType, IamClient> iamClients;

  public PermissionsApplier(ImmutableMap<ResourceType, IamClient> iamClients) {
    this.iamClients = iamClients;
  }

  /** Applies a list of permissions to GCS managed folders. */
  public void apply(Permissions permissions, ExtraPermissions keep) throws IOException {
    Map<GcpResource, Map<Role, Set<Identity>>> pathToBindingsMapping =
        permissions.permissions().stream()
            .collect(
                Collectors.groupingBy(
                    iamBinding ->
                        GcpResource.create(iamBinding.resourceType(), iamBinding.resourcePath()),
                    Collectors.groupingBy(
                        iamBinding -> Role.of(iamBinding.role()),
                        Collectors.mapping(
                            iamBinding -> Identity.valueOf(iamBinding.principal()),
                            Collectors.toSet()))));

    for (Map.Entry<GcpResource, Map<Role, Set<Identity>>> entry :
        pathToBindingsMapping.entrySet()) {
      GcpResource resource = entry.getKey();
      Map<Role, Set<Identity>> bindings = entry.getValue();
      IamClient iamClient = iamClients.get(resource.type());
      if (iamClient == null) {
        throw new IllegalArgumentException("Invalid resource type " + resource.type());
      }
      iamClient.addIamPolicyBindings(resource.path(), bindings, keep);
    }
  }
}
