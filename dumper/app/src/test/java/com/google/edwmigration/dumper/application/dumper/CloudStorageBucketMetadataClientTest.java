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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.OutputBucketVerifier.ListedBucket;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CloudStorageBucketMetadataClientTest {

  private static final int FORBIDDEN = 403;

  private WireMockServer server;
  private CloudStorageBucketMetadataClient client;

  @Before
  public void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    StorageOptions storageOptions =
        StorageOptions.newBuilder()
            .setHost(server.baseUrl())
            .setProjectId("unused-project")
            .setCredentials(NoCredentials.getInstance())
            .setRetrySettings(ServiceOptions.getNoRetrySettings())
            .build();
    client = new CloudStorageBucketMetadataClient(storageOptions);
  }

  @After
  public void tearDown() {
    server.stop();
  }

  @Test
  public void listBuckets_queriesGivenProjectAndPrefix() throws IOException {
    server.stubFor(
        get(urlPathEqualTo("/storage/v1/b"))
            .withQueryParam("project", equalTo("customer-project"))
            .withQueryParam("prefix", equalTo("customer-bucket"))
            .willReturn(
                okJson(
                    "{\"kind\": \"storage#buckets\", \"items\": ["
                        + bucketJson("customer-bucket", "enforced")
                        + ", "
                        + bucketJson("customer-bucket-2", "inherited")
                        + ", {\"name\": \"customer-bucket-3\"}]}")));

    ImmutableList<ListedBucket> buckets = client.listBuckets("customer-project", "customer-bucket");

    assertEquals(
        ImmutableList.of(
            ListedBucket.create("customer-bucket", true),
            ListedBucket.create("customer-bucket-2", false),
            ListedBucket.create("customer-bucket-3", false)),
        buckets);
  }

  @Test
  public void listBuckets_forbidden_throwsIOException() {
    server.stubFor(
        get(urlPathEqualTo("/storage/v1/b"))
            .willReturn(
                aResponse()
                    .withStatus(FORBIDDEN)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\": {\"code\": 403, \"message\": \"Forbidden\"}}")));

    assertThrows(
        IOException.class, () -> client.listBuckets("customer-project", "customer-bucket"));
  }

  @Test
  public void fetchIamMembers_returnsMembersOfAllBindingsIncludingConditional() throws IOException {
    server.stubFor(
        get(urlPathEqualTo("/storage/v1/b/customer-bucket/iam"))
            .withQueryParam("optionsRequestedPolicyVersion", equalTo("3"))
            .willReturn(
                okJson(
                    "{\"kind\": \"storage#policy\", \"version\": 3, \"bindings\": ["
                        + "{\"role\": \"roles/storage.legacyBucketOwner\","
                        + " \"members\": [\"projectOwner:customer-project\"]},"
                        + "{\"role\": \"roles/storage.objectViewer\","
                        + " \"members\": [\"allUsers\"],"
                        + " \"condition\": {\"title\": \"reports\","
                        + " \"expression\": \"resource.name.startsWith('x')\"}},"
                        + "{\"role\": \"roles/storage.objectCreator\","
                        + " \"members\": [\"user:someone@example.com\"]}]}")));

    ImmutableSet<String> members = client.fetchIamMembers("customer-bucket");

    assertEquals(
        ImmutableSet.of("projectOwner:customer-project", "allUsers", "user:someone@example.com"),
        members);
  }

  @Test
  public void fetchIamMembers_forbidden_throwsIOException() {
    server.stubFor(
        get(urlPathEqualTo("/storage/v1/b/customer-bucket/iam"))
            .willReturn(
                aResponse()
                    .withStatus(FORBIDDEN)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\": {\"code\": 403, \"message\": \"Forbidden\"}}")));

    assertThrows(IOException.class, () -> client.fetchIamMembers("customer-bucket"));
  }

  private static String bucketJson(String name, String publicAccessPrevention) {
    return String.format(
        "{\"name\": \"%s\", \"iamConfiguration\": {"
            + "\"bucketPolicyOnly\": {\"enabled\": true},"
            + " \"uniformBucketLevelAccess\": {\"enabled\": true},"
            + " \"publicAccessPrevention\": \"%s\"}}",
        name, publicAccessPrevention);
  }
}
