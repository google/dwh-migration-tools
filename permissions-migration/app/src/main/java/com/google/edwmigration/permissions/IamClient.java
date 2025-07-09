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
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/** An interface to model a generic GCP IAM client. */
public interface IamClient extends AutoCloseable {

  @AutoValue
  abstract class GcpResource {

    public abstract ResourceType type();

    public abstract String path();

    public static GcpResource create(ResourceType resourceType, String path) {
      return new AutoValue_IamClient_GcpResource(resourceType, path);
    }
  }

  /**
   * Adds multiple IAM Policy bindings from role to identity to given path. When extraPermissions is
   * ExtraPermissions.KEEP then existing permissions are preserved. When extraPermissions is
   * ExtraPermissions.PURGE then existing permissions are removed.
   */
  void addIamPolicyBindings(
      String path, Map<Role, Set<Identity>> bindings, ExtraPermissions extraPermissions)
      throws IOException;
}
