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

import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.permissions.BqTableClient;
import com.google.edwmigration.permissions.ExtraPermissions;
import com.google.edwmigration.permissions.GcsManagedFoldersClient;
import com.google.edwmigration.permissions.PermissionsParser;
import com.google.edwmigration.permissions.files.FileProcessor;
import com.google.edwmigration.permissions.models.IamBinding.ResourceType;
import com.google.edwmigration.permissions.models.Permissions;
import java.io.InputStream;
import java.nio.file.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates GCS managed folders defined in permissions.yaml file and applies the IAM policies. */
public class ApplyCommand {

  private static final Logger LOG = LoggerFactory.getLogger(ApplyCommand.class);

  public void run(String[] args) throws Exception {
    ApplyOptions applyOptions = new ApplyOptions(args);
    if (applyOptions.handleHelp()) {
      return;
    }

    ExtraPermissions extraPermissions = applyOptions.getExtraPermissions();
    String permissionsFilename = applyOptions.getPermissionsFilename();

    try (GcsManagedFoldersClient managedFoldersClient = GcsManagedFoldersClient.create();
        BqTableClient bqTableClient = BqTableClient.create()) {
      PermissionsApplier permissionsApplier =
          new PermissionsApplier(
              ImmutableMap.of(
                  ResourceType.GCS_MANAGED_FOLDER, GcsManagedFoldersClient.create(),
                  ResourceType.BQ_TABLE, BqTableClient.create()));
      PermissionsParser permissionsParser = new PermissionsParser();

      FileProcessor.applyConsumer(
          permissionsFilename,
          path -> {
            LOG.info("Reading permissions from: {}", path);
            try (InputStream inputStream = Files.newInputStream(path)) {
              Permissions permissions = permissionsParser.Parse(inputStream);
              permissionsApplier.apply(permissions, extraPermissions);
            }
          });
    }
  }
}
