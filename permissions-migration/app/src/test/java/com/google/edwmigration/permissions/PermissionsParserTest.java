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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.edwmigration.permissions.models.IamBinding;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Permissions;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class PermissionsParserTest {

  @Test
  public void parse_success() throws IOException {
    PermissionsParser permissionsParser = new PermissionsParser();
    Permissions result;

    try (InputStream yamlStream =
        Resources.asByteSource(Resources.getResource("permissions.yaml")).openStream()) {
      result = permissionsParser.Parse(yamlStream);
    }

    Permissions expected =
        Permissions.create(
            ImmutableList.of(
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-1-bucket/table-a")
                    .principal("user:user2@example.com")
                    .role("roles/storage.objectViewer")
                    .priority(10)
                    .build(),
                IamBinding.builder()
                    .resourceType(ResourceType.GCS_MANAGED_FOLDER)
                    .resourcePath("gs://cluster-2-bucket/table-a")
                    .principal("user:user1@example.com")
                    .role("roles/storage.objectUser")
                    .priority(10)
                    .build()));
    assertThat(result).isEqualTo(expected);
  }
}
