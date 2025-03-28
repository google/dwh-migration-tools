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

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.cloud.Identity;
import com.google.cloud.Policy;
import com.google.cloud.Role;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.storage.control.v2.BucketName;
import com.google.storage.control.v2.CreateManagedFolderRequest;
import com.google.storage.control.v2.ManagedFolder;
import com.google.storage.control.v2.StorageControlClient;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles calls to GCS API related to managed folders. */
public class GcsManagedFoldersClient implements IamClient {

  private final Storage storage;
  private final StorageControlClient storageControlClient;
  private static final Logger LOG = LoggerFactory.getLogger(GcsManagedFoldersClient.class);

  // Alias for global namespace which can be used in place of `{project}` in
  // bucket resource name in some RPCs.
  private static final String GLOBAL_NAMESPACE_ALIAS = "_";

  private GcsManagedFoldersClient(Storage storage, StorageControlClient storageControlClient) {
    this.storage = storage;
    this.storageControlClient = storageControlClient;
  }

  public static GcsManagedFoldersClient create() throws IOException {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    StorageControlClient storageControlClient = StorageControlClient.create();
    return new GcsManagedFoldersClient(storage, storageControlClient);
  }

  public static GcsManagedFoldersClient create(String projectId) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    StorageControlClient storageControlClient = StorageControlClient.create();
    return new GcsManagedFoldersClient(storage, storageControlClient);
  }

  /** Creates a new managed folder. */
  public void createManagedFolderIfDoesntExist(GcsPath path) throws IOException {
    CreateManagedFolderRequest request =
        CreateManagedFolderRequest.newBuilder()
            .setParent(BucketName.of(GLOBAL_NAMESPACE_ALIAS, path.bucketName()).toString())
            .setManagedFolderId(path.objectName())
            .build();

    try {
      ManagedFolder createdManagedFolder = storageControlClient.createManagedFolder(request);
      LOG.info("Created Managed Folder: {}", createdManagedFolder.getName());
    } catch (FailedPreconditionException | AlreadyExistsException e) {
      LOG.debug("The Managed Folder already exists: {}", path);
    }
  }

  private void addIdentityToIamPolicy(GcsPath path, Role role, Identity identity) {
    String managedFolderNameForIamCall = getManagedFolderNameForIamCall(path);

    Policy policy =
        createBuilderFromExistingPolicy(managedFolderNameForIamCall, path)
            .addIdentity(role, identity)
            .build();
    storage.setIamPolicy(managedFolderNameForIamCall, policy);
  }

  public void addIamPolicyBindings(
      String path, Map<Role, Set<Identity>> bindings, ExtraPermissions extraPermissions)
      throws IOException {
    GcsPath gcsPath = GcsPath.parse(path);
    createManagedFolderIfDoesntExist(gcsPath);
    String managedFolderNameForIamCall = getManagedFolderNameForIamCall(gcsPath);
    Policy.Builder policyBuilder =
        extraPermissions == ExtraPermissions.KEEP
            ? createBuilderFromExistingPolicy(managedFolderNameForIamCall, gcsPath)
            : createEmptyPolicyBuilder();
    bindings.forEach(
        (role, identities) ->
            identities.forEach((identity) -> policyBuilder.addIdentity(role, identity)));
    LOG.info("Apply policy {} to folder {}", bindings, gcsPath.toString());
    storage.setIamPolicy(managedFolderNameForIamCall, policyBuilder.build());
  }

  private Policy.Builder createBuilderFromExistingPolicy(
      String managedFolderNameForIamCall, GcsPath path) {
    try {
      // We abuse the API here - storage.getIamPolicy should be used for buckets but we use it for
      // managed folder here.
      // This seems to work correctly when the policy exists.
      // When the policy does not exist, it throws NullPointerException.
      // I am not happy with this solution - it would be best to replace it with some dedicated API
      // for managed folders.
      LOG.debug("Retrieved an existing policy for path: {}", path);
      return storage.getIamPolicy(managedFolderNameForIamCall).toBuilder();
    } catch (NullPointerException expected) {
      LOG.debug("Could not get an existing policy for path: {}", path);
      return createEmptyPolicyBuilder();
    }
  }

  private Policy.Builder createEmptyPolicyBuilder() {
    return Policy.newBuilder().setVersion(1);
  }

  /**
   * Creates a string that can be used in `Storage.getIamPolicy` and `Storage.setIamPolicy` to
   * operate on managed folders instead of buckets. We should find w better way to handle that
   * (different API?).
   */
  private static String getManagedFolderNameForIamCall(GcsPath path) {
    return path.bucketName() + "/managedFolders/" + path.objectName();
  }

  @Override
  public void close() throws Exception {
    storage.close();
    storageControlClient.close();
  }
}
