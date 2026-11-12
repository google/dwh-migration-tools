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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OutputBucketVerifierTest {

  private static final String PROJECT = "customer-project";
  private static final String BUCKET = "customer-bucket";

  @Test
  public void verify_ownedBucketWithPublicAccessPreventionEnforced_skipsIamPolicy() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT).withBucket(BUCKET, /* enforced= */ true);

    new OutputBucketVerifier(client, PROJECT).verify(BUCKET);

    assertEquals(ImmutableList.of(PROJECT + ":" + BUCKET), client.listCalls);
    assertTrue(client.iamCalls.isEmpty());
  }

  @Test
  public void verify_ownedBucketWithPrivateIamPolicy_passes() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT)
            .withBucket(BUCKET, /* enforced= */ false)
            .withIamMembers("projectOwner:" + PROJECT, "user:someone@example.com");

    new OutputBucketVerifier(client, PROJECT).verify(BUCKET);

    assertEquals(ImmutableList.of(BUCKET), client.iamCalls);
  }

  @Test
  public void verify_bucketNotInProject_throws() {
    FakeBucketMetadataClient client = new FakeBucketMetadataClient(PROJECT);

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));

    assertMessageContains(exception, "gs://" + BUCKET, PROJECT, "--skip-bucket-verification");
    assertTrue(client.iamCalls.isEmpty());
  }

  @Test
  public void verify_onlyLongerNameWithSamePrefixInProject_throws() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT).withBucket(BUCKET + "-old", /* enforced= */ true);

    assertThrows(
        MetadataDumperUsageException.class,
        () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));
  }

  @Test
  public void verify_bucketGrantsAllUsers_throws() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT)
            .withBucket(BUCKET, /* enforced= */ false)
            .withIamMembers("projectOwner:" + PROJECT, "allUsers");

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));

    assertMessageContains(exception, "gs://" + BUCKET, "allUsers");
  }

  @Test
  public void verify_bucketGrantsAllAuthenticatedUsers_throws() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT)
            .withBucket(BUCKET, /* enforced= */ false)
            .withIamMembers("allAuthenticatedUsers");

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));

    assertMessageContains(exception, "gs://" + BUCKET, "allAuthenticatedUsers");
  }

  @Test
  public void verify_listingDenied_throws() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT)
            .withBucket(BUCKET, /* enforced= */ true)
            .failingList(new IOException("403 Forbidden"));

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));

    assertMessageContains(
        exception, "gs://" + BUCKET, PROJECT, "403 Forbidden", "storage.buckets.list");
  }

  @Test
  public void verify_iamPolicyReadDenied_throws() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT)
            .withBucket(BUCKET, /* enforced= */ false)
            .failingIam(new IOException("403 Forbidden"));

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, PROJECT).verify(BUCKET));

    assertMessageContains(
        exception, "gs://" + BUCKET, "403 Forbidden", "storage.buckets.getIamPolicy");
  }

  @Test
  public void verify_noProject_throwsWithoutCallingCloudStorage() {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient(PROJECT).withBucket(BUCKET, /* enforced= */ true);

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> new OutputBucketVerifier(client, /* projectId= */ null).verify(BUCKET));

    assertMessageContains(exception, "gs://" + BUCKET, "--bucket-owner-project");
    assertTrue(client.listCalls.isEmpty());
  }

  private static void assertMessageContains(Exception exception, String... parts) {
    String message = exception.getMessage();
    for (String part : parts) {
      assertTrue("Expected '" + part + "' in: " + message, message.contains(part));
    }
  }
}
