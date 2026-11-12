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
package com.google.edwmigration.dumper.application.dumper;

import com.google.cloud.Binding;
import com.google.cloud.Policy;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo.IamConfiguration;
import com.google.cloud.storage.BucketInfo.PublicAccessPrevention;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.BucketSourceOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.OutputBucketVerifier.BucketMetadataClient;
import com.google.edwmigration.dumper.application.dumper.OutputBucketVerifier.ListedBucket;
import java.io.IOException;
import javax.annotation.Nonnull;

/** Reads bucket metadata through the Cloud Storage JSON API. */
class CloudStorageBucketMetadataClient implements BucketMetadataClient {

  /** Version 3 returns conditional role bindings too, so none of them are missed. */
  private static final long IAM_POLICY_VERSION = 3;

  private final StorageOptions storageOptions;

  CloudStorageBucketMetadataClient(@Nonnull StorageOptions storageOptions) {
    this.storageOptions = storageOptions;
  }

  @Nonnull
  @Override
  public ImmutableList<ListedBucket> listBuckets(@Nonnull String projectId, @Nonnull String prefix)
      throws IOException {
    ImmutableList.Builder<ListedBucket> buckets = ImmutableList.builder();
    try (Storage storage =
        storageOptions.toBuilder().setProjectId(projectId).build().getService()) {
      for (Bucket bucket : storage.list(BucketListOption.prefix(prefix)).iterateAll()) {
        buckets.add(
            ListedBucket.create(bucket.getName(), isPublicAccessPreventionEnforced(bucket)));
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return buckets.build();
  }

  @Nonnull
  @Override
  public ImmutableSet<String> fetchIamMembers(@Nonnull String bucketName) throws IOException {
    ImmutableSet.Builder<String> members = ImmutableSet.builder();
    try (Storage storage = storageOptions.toBuilder().build().getService()) {
      Policy policy =
          storage.getIamPolicy(
              bucketName, BucketSourceOption.requestedPolicyVersion(IAM_POLICY_VERSION));
      for (Binding binding : policy.getBindingsList()) {
        members.addAll(binding.getMembers());
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
    return members.build();
  }

  private static boolean isPublicAccessPreventionEnforced(Bucket bucket) {
    IamConfiguration iamConfiguration = bucket.getIamConfiguration();
    return iamConfiguration != null
        && iamConfiguration.getPublicAccessPrevention() == PublicAccessPrevention.ENFORCED;
  }
}
