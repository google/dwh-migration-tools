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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.OutputBucketVerifier.BucketMetadataClient;
import com.google.edwmigration.dumper.application.dumper.OutputBucketVerifier.ListedBucket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/** In-memory {@link BucketMetadataClient} for one project. Records the calls it receives. */
class FakeBucketMetadataClient implements BucketMetadataClient {

  private final String projectId;
  private final List<ListedBucket> buckets = new ArrayList<>();
  private ImmutableSet<String> iamMembers = ImmutableSet.of();
  @Nullable private IOException listFailure;
  @Nullable private IOException iamFailure;

  final List<String> listCalls = new ArrayList<>();
  final List<String> iamCalls = new ArrayList<>();

  FakeBucketMetadataClient(String projectId) {
    this.projectId = projectId;
  }

  FakeBucketMetadataClient withBucket(String name, boolean publicAccessPreventionEnforced) {
    buckets.add(ListedBucket.create(name, publicAccessPreventionEnforced));
    return this;
  }

  FakeBucketMetadataClient withIamMembers(String... members) {
    this.iamMembers = ImmutableSet.copyOf(members);
    return this;
  }

  FakeBucketMetadataClient failingList(IOException failure) {
    this.listFailure = failure;
    return this;
  }

  FakeBucketMetadataClient failingIam(IOException failure) {
    this.iamFailure = failure;
    return this;
  }

  @Override
  public ImmutableList<ListedBucket> listBuckets(String projectId, String prefix)
      throws IOException {
    listCalls.add(projectId + ":" + prefix);
    if (listFailure != null) {
      throw listFailure;
    }

    if (!this.projectId.equals(projectId)) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<ListedBucket> matching = ImmutableList.builder();
    for (ListedBucket bucket : buckets) {
      if (bucket.name().startsWith(prefix)) {
        matching.add(bucket);
      }
    }
    return matching.build();
  }

  @Override
  public ImmutableSet<String> fetchIamMembers(String bucketName) throws IOException {
    iamCalls.add(bucketName);
    if (iamFailure != null) {
      throw iamFailure;
    }

    return iamMembers;
  }
}
