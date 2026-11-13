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
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.bigquery.BigQueryLogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.test.TestConnector;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import joptsimple.OptionException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class MetadataDumperTest {
  private static final String DEFAULT_FILENAME = "dwh-migration-test-metadata.zip";

  private enum TopLevelTestFile {
    DEFAULT_FILENAME_TEST_FILE(DEFAULT_FILENAME),
    SAMPLE_DIR("sample_dir"),
    TEST_ZIP("test.zip"),
    DIR_ZIP("dir.zip"),
    TEST("test");

    final String filename;

    TopLevelTestFile(String filename) {
      this.filename = filename;
    }
  }

  private final Connector connector = new TestConnector();

  @Before
  public void setUp() throws IOException {
    deleteTopLevelTestFiles();
  }

  @After
  public void tearDown() throws IOException {
    deleteTopLevelTestFiles();
  }

  private static void deleteTopLevelTestFiles() throws IOException {
    for (TopLevelTestFile topLevelTestFile : TopLevelTestFile.values()) {
      File file = new File(topLevelTestFile.filename);
      if (file.exists()) {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
  }

  @Test
  public void testInstantiate() throws Exception {
    boolean result =
        new MetadataDumper("--connector", new BigQueryLogsConnector().getName(), "--dry-run").run();
    assertTrue(result);
  }

  @Test
  public void testCreatesDefaultOutputZip() throws Exception {
    File expectedFile = new File(DEFAULT_FILENAME);

    // Act
    new MetadataDumper("--connector", connector.getName()).run();

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesDefaultOutputZipInProvidedDirectory() throws Exception {
    String path = TopLevelTestFile.SAMPLE_DIR.filename;
    File expectedFile = new File(path, DEFAULT_FILENAME);

    // Act
    new MetadataDumper("--connector", connector.getName(), "--output", path).run();

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesZipWithGivenName() throws Exception {
    String name = TopLevelTestFile.TEST_ZIP.filename;
    File expectedFile = new File(name);

    // Act
    new MetadataDumper("--connector", connector.getName(), "--output", name).run();

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testOverridesZipWithDefaultName() throws Exception {
    File expectedFile = new File(TopLevelTestFile.SAMPLE_DIR.filename + "/" + DEFAULT_FILENAME);
    String dirName = TopLevelTestFile.SAMPLE_DIR.filename;
    File dir = new File(dirName);

    // Act
    dir.mkdirs();
    new MetadataDumper("--connector", connector.getName(), "--output", dirName).run();

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesFileInsideFolderNameWithZip() throws Exception {
    File expectedFile = new File(TopLevelTestFile.DIR_ZIP.filename + "/" + DEFAULT_FILENAME);
    String dirName = TopLevelTestFile.DIR_ZIP.filename;
    File dir = new File(dirName);

    // Act
    dir.mkdirs();
    new MetadataDumper("--connector", connector.getName(), "--output", dirName).run();

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testDoesNotOverrideFileWithDirectory() throws IOException {
    String filename = TopLevelTestFile.TEST.filename;
    File file = new File(filename);

    // Act
    file.createNewFile();
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                new MetadataDumper("--connector", connector.getName(), "--output", filename).run());

    // Assert
    assertTrue(exception.getMessage().startsWith("A file already exists at test"));
  }

  @Test
  public void testFailsOnUnrecognizedFlag() {
    OptionException exception =
        assertThrows(
            OptionException.class,
            () -> new MetadataDumper("--unrecognized-flag", "random-value").run());

    // Assert
    assertEquals("unrecognized-flag is not a recognized option", exception.getMessage());
  }

  @Test
  public void testFailsOnUnrecognizedDialect() {
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () ->
                new MetadataDumper(
                        "--connector", connector.getName(), "-DImaginaryDialect.flag=random-value")
                    .run());

    // Assert
    assertEquals(
        "Unknown property: name='ImaginaryDialect.flag', value='random-value'",
        exception.getMessage());
  }

  @Test
  public void testFailsOnUnrecognizedFlagForSpecificDialect() {
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () ->
                new MetadataDumper(
                        "--connector", connector.getName(), "-Dhiveql.rpc.protection=privacy")
                    .run());

    // Assert
    assertEquals(
        "Property: name='hiveql.rpc.protection', value='privacy' is not compatible with connector"
            + " 'test'",
        exception.getMessage());
  }

  @Test
  public void testAcceptsValidFlagsForSpecificDialect() throws Exception {
    File file = new File(DEFAULT_FILENAME);

    // Act
    new MetadataDumper("--connector", connector.getName(), "-Dtest.test.property=test-value").run();

    // Assert
    assertTrue(file.exists());
  }

  @Test
  public void run_gcsBucketNotInProject_failsBeforeOpeningBucket() throws Exception {
    FakeBucketMetadataClient client = new FakeBucketMetadataClient("customer-project");
    List<String> shutdownCalls = new ArrayList<>();
    MetadataDumper dumper =
        new MetadataDumper(
            shutdownCalls::add,
            verifierWithDefaultProject(client, "customer-project"),
            "--connector",
            connector.getName(),
            "--output",
            "gs://other-bucket/extracts/");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, dumper::run);

    assertTrue(exception.getMessage().contains("gs://other-bucket"));
    assertEquals(ImmutableList.of("customer-project:other-bucket"), client.listCalls);
    assertTrue(shutdownCalls.isEmpty());
  }

  @Test
  public void run_gcsOutputWithDryRun_doesNotVerifyBucket() throws Exception {
    boolean result =
        new MetadataDumper(
                outputFile -> {},
                MetadataDumperTest::failVerifierCreation,
                "--connector",
                connector.getName(),
                "--output",
                "gs://customer-bucket/extracts/",
                "--dry-run")
            .run();

    assertTrue(result);
  }

  @Test
  public void verifyOutputBucket_ownedPrivateBucket_passes() throws Exception {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient("customer-project")
            .withBucket("customer-bucket", /* publicAccessPreventionEnforced= */ true);
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            verifierWithDefaultProject(client, "customer-project"),
            "--connector",
            connector.getName());

    dumper.verifyOutputBucket("gs://customer-bucket/extracts/" + DEFAULT_FILENAME);

    assertEquals(ImmutableList.of("customer-project:customer-bucket"), client.listCalls);
  }

  @Test
  public void verifyOutputBucket_skipFlag_doesNotVerify() throws Exception {
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            MetadataDumperTest::failVerifierCreation,
            "--connector",
            connector.getName(),
            "--skip-bucket-verification");

    dumper.verifyOutputBucket("gs://other-bucket/extracts/" + DEFAULT_FILENAME);
  }

  @Test
  public void verifyOutputBucket_localOutput_doesNotVerify() throws Exception {
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            MetadataDumperTest::failVerifierCreation,
            "--connector",
            connector.getName());

    dumper.verifyOutputBucket(DEFAULT_FILENAME);
  }

  @Test
  public void verifyOutputBucket_ownerProjectFlag_checksGivenProject() throws Exception {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient("owner-project")
            .withBucket("customer-bucket", /* publicAccessPreventionEnforced= */ true);
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            verifierWithDefaultProject(client, "customer-project"),
            "--connector",
            connector.getName(),
            "--bucket-owner-project",
            "owner-project");

    dumper.verifyOutputBucket("gs://customer-bucket/extracts/" + DEFAULT_FILENAME);

    assertEquals(ImmutableList.of("owner-project:customer-bucket"), client.listCalls);
  }

  @Test
  public void verifyOutputBucket_emptyOwnerProject_checksDefaultProject() throws Exception {
    FakeBucketMetadataClient client =
        new FakeBucketMetadataClient("customer-project")
            .withBucket("customer-bucket", /* publicAccessPreventionEnforced= */ true);
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            verifierWithDefaultProject(client, "customer-project"),
            "--connector",
            connector.getName(),
            "--bucket-owner-project",
            "");

    dumper.verifyOutputBucket("gs://customer-bucket/extracts/" + DEFAULT_FILENAME);

    assertEquals(ImmutableList.of("customer-project:customer-bucket"), client.listCalls);
  }

  @Test
  public void verifyOutputBucket_ownerProjectWithLocalOutput_throws() throws Exception {
    MetadataDumper dumper =
        new MetadataDumper(
            outputFile -> {},
            MetadataDumperTest::failVerifierCreation,
            "--connector",
            connector.getName(),
            "--bucket-owner-project",
            "owner-project");

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class, () -> dumper.verifyOutputBucket(DEFAULT_FILENAME));

    assertTrue(exception.getMessage().startsWith("--bucket-owner-project can only be used"));
  }

  /** Mirrors {@link OutputBucketVerifier#createWithDefaultCredentials} with a fake client. */
  private static Function<String, OutputBucketVerifier> verifierWithDefaultProject(
      FakeBucketMetadataClient client, String defaultProject) {
    return ownerProject ->
        new OutputBucketVerifier(client, ownerProject != null ? ownerProject : defaultProject);
  }

  private static OutputBucketVerifier failVerifierCreation(String ownerProject) {
    throw new AssertionError("The output bucket must not be verified.");
  }
}
