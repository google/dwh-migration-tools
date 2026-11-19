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

import static com.google.edwmigration.dumper.application.dumper.ConnectorArguments.OPT_SKIP_BUCKET_VERIFICATION;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks a Cloud Storage output bucket before the dumper writes to it.
 *
 * <p>The bucket must be listed among the buckets of the caller's Google Cloud project, and it must
 * not grant access to {@code allUsers} or {@code allAuthenticatedUsers}. A bucket with Public
 * Access Prevention enforced cannot be made public, so its IAM policy is not read. Any check that
 * cannot be completed, for example for lack of permission, fails the verification.
 *
 * <p>The bucket must belong to the project given by {@code --bucket-owner-project}. Without it, the
 * caller's project is the default project of the Google Cloud client libraries. It is the first one
 * found of: the {@code GOOGLE_CLOUD_PROJECT} or {@code GCLOUD_PROJECT} environment variable, the
 * App Engine project, the {@code project_id} of the service account key named by {@code
 * GOOGLE_APPLICATION_CREDENTIALS}, the project of the active gcloud configuration file, and the
 * Compute Engine project. The {@code CLOUDSDK_CORE_PROJECT} variable and gcloud's {@code --project}
 * flag are not read.
 */
class OutputBucketVerifier {

  private static final Logger logger = LoggerFactory.getLogger(OutputBucketVerifier.class);

  private static final ImmutableSet<String> PUBLIC_MEMBERS =
      ImmutableSet.of("allUsers", "allAuthenticatedUsers");

  private static final String SKIP_HINT =
      "pass --" + OPT_SKIP_BUCKET_VERIFICATION + " if the bucket is known to be the right one.";

  /** The Cloud Storage calls the verification needs. */
  interface BucketMetadataClient {

    /** Lists the buckets of the project whose names start with the prefix. */
    @Nonnull
    ImmutableList<ListedBucket> listBuckets(@Nonnull String projectId, @Nonnull String prefix)
        throws IOException;

    /** Returns the members of every role binding in the bucket's IAM policy. */
    @Nonnull
    ImmutableSet<String> fetchIamMembers(@Nonnull String bucketName) throws IOException;
  }

  /** A bucket as returned by a bucket listing. */
  @AutoValue
  abstract static class ListedBucket {

    abstract String name();

    abstract boolean publicAccessPreventionEnforced();

    static ListedBucket create(String name, boolean publicAccessPreventionEnforced) {
      return new AutoValue_OutputBucketVerifier_ListedBucket(name, publicAccessPreventionEnforced);
    }
  }

  private final BucketMetadataClient client;
  @Nullable private final String projectId;

  OutputBucketVerifier(@Nonnull BucketMetadataClient client, @Nullable String projectId) {
    this.client = client;
    this.projectId = projectId;
  }

  /**
   * Creates a verifier that uses the application default credentials.
   *
   * @param ownerProjectId the project that must own the bucket, or {@code null} for the default
   *     project
   */
  static OutputBucketVerifier createWithDefaultCredentials(@Nullable String ownerProjectId) {
    return new OutputBucketVerifier(
        new CloudStorageBucketMetadataClient(StorageOptions.getDefaultInstance()),
        ownerProjectId != null ? ownerProjectId : ServiceOptions.getDefaultProjectId());
  }

  /**
   * Verifies the bucket.
   *
   * @throws MetadataDumperUsageException if the bucket is not in the caller's project, is public,
   *     or cannot be checked.
   */
  void verify(@Nonnull String bucketName) {
    if (projectId == null) {
      throw new MetadataDumperUsageException(
          String.format(
              "Cannot verify output bucket gs://%s: no Google Cloud project is set. Pass the"
                  + " project that owns the bucket with --%s, or %s",
              bucketName, ConnectorArguments.OPT_BUCKET_OWNER_PROJECT, SKIP_HINT));
    }

    logger.info("Verifying output bucket gs://{} in project {}.", bucketName, projectId);
    ListedBucket bucket = findInProject(bucketName, projectId);
    if (!bucket.publicAccessPreventionEnforced()) {
      verifyNotPublic(bucketName);
    }

    logger.info(
        "Output bucket gs://{} belongs to project {} and is not public.", bucketName, projectId);
  }

  @Nonnull
  private ListedBucket findInProject(@Nonnull String bucketName, @Nonnull String projectId) {
    ImmutableList<ListedBucket> buckets;
    try {
      buckets = client.listBuckets(projectId, bucketName);
    } catch (IOException e) {
      throw new MetadataDumperUsageException(
          String.format(
              "Cannot verify that output bucket gs://%s belongs to project %s: listing the"
                  + " buckets of the project failed (%s). This needs the storage.buckets.list"
                  + " permission on the project. Grant it, pass the project that owns the bucket"
                  + " with --%s, or %s",
              bucketName,
              projectId,
              e.getMessage(),
              ConnectorArguments.OPT_BUCKET_OWNER_PROJECT,
              SKIP_HINT));
    }

    for (ListedBucket bucket : buckets) {
      if (bucket.name().equals(bucketName)) {
        return bucket;
      }
    }

    throw new MetadataDumperUsageException(
        String.format(
            "Output bucket gs://%s does not belong to project %s. Check the bucket name in --%s,"
                + " pass the project that owns the bucket with --%s, or %s",
            bucketName,
            projectId,
            ConnectorArguments.OPT_OUTPUT,
            ConnectorArguments.OPT_BUCKET_OWNER_PROJECT,
            SKIP_HINT));
  }

  private void verifyNotPublic(@Nonnull String bucketName) {
    ImmutableSet<String> members;
    try {
      members = client.fetchIamMembers(bucketName);
    } catch (IOException e) {
      throw new MetadataDumperUsageException(
          String.format(
              "Cannot verify that output bucket gs://%s is not public: Public Access Prevention is"
                  + " not enforced on the bucket and reading its IAM policy failed (%s). Enforce"
                  + " Public Access Prevention on the bucket, grant the"
                  + " storage.buckets.getIamPolicy permission, or %s",
              bucketName, e.getMessage(), SKIP_HINT));
    }

    for (String member : members) {
      if (PUBLIC_MEMBERS.contains(member)) {
        throw new MetadataDumperUsageException(
            String.format(
                "Output bucket gs://%s grants access to %s. Remove public access from the bucket,"
                    + " or %s",
                bucketName, member, SKIP_HINT));
      }
    }
  }
}
