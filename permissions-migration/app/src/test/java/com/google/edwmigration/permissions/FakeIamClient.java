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
package com.google.edwmigration.permissions;

import com.google.auto.value.AutoValue;
import com.google.cloud.Identity;
import com.google.cloud.Role;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** A fake implementation of FoldersClient for a purpose of testing. */
public class FakeIamClient implements IamClient {

  @AutoValue
  public abstract static class Permission {

    public static Permission create(Role role, Identity identity) {
      return new AutoValue_FakeIamClient_Permission(role, identity);
    }

    public abstract Role role();

    public abstract Identity identity();
  }

  private final Map<String, Set<Permission>> objectPermissions = new HashMap<>();

  @Override
  public void addIamPolicyBindings(
      String path, Map<Role, Set<Identity>> bindings, ExtraPermissions extraPermissions) {
    if (extraPermissions == ExtraPermissions.PURGE) {
      objectPermissions.get(path).clear();
    }
    bindings.forEach(
        (role, identities) ->
            identities.forEach((identity) -> addIdentityToIamPolicy(path, role, identity)));
  }

  public void addIdentityToIamPolicy(String path, Role role, Identity identity) {
    objectPermissions
        .computeIfAbsent(path, key -> new HashSet<>())
        .add(Permission.create(role, identity));
  }

  /** Returns true only if a folder with given path has been created before. */
  public boolean checkPathExists(String path) {
    return objectPermissions.containsKey(path);
  }

  /** Returns true only if a permission with the given role and identity has been added before. */
  public boolean checkPermissionExists(String path, Role role, Identity identity) {
    Permission expectedPermission = Permission.create(role, identity);
    return objectPermissions.containsKey(path)
        && objectPermissions.get(path).contains(expectedPermission);
  }

  @Override
  public void close() {}
}
