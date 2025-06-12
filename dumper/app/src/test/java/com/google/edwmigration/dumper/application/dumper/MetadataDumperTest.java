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

import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.bigquery.BigQueryLogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.test.TestConnector;
import java.io.File;
import java.io.IOException;
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

  private Main dumper = new Main(new MetadataDumper(new DumperRunMetricsGenerator()));
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
    boolean result = dumper.run("--connector", new BigQueryLogsConnector().getName(), "--dry-run");
    assertTrue(result);
  }

  @Test
  public void testCreatesDefaultOutputZip() throws Exception {
    File expectedFile = new File(DEFAULT_FILENAME);

    // Act
    dumper.run("--connector", connector.getName());

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesDefaultOutputZipInProvidedDirectory() throws Exception {
    String path = TopLevelTestFile.SAMPLE_DIR.filename;
    File expectedFile = new File(path, DEFAULT_FILENAME);

    // Act
    dumper.run("--connector", connector.getName(), "--output", path);

    // Assert
    assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesZipWithGivenName() throws Exception {
    String name = TopLevelTestFile.TEST_ZIP.filename;
    File expectedFile = new File(name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", name);

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
    dumper.run("--connector", connector.getName(), "--output", dirName);

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
    dumper.run("--connector", connector.getName(), "--output", dirName);

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
            () -> dumper.run("--connector", connector.getName(), "--output", filename));

    // Assert
    assertTrue(exception.getMessage().startsWith("A file already exists at test"));
  }

  @Test
  public void testFailsOnUnrecognizedFlag() {
    OptionException exception =
        assertThrows(
            OptionException.class, () -> dumper.run("--unrecognized-flag", "random-value"));

    // Assert
    assertEquals("unrecognized-flag is not a recognized option", exception.getMessage());
  }

  @Test
  public void testFailsOnUnrecognizedDialect() {
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () ->
                dumper.run(
                    "--connector", connector.getName(), "-DImaginaryDialect.flag=random-value"));

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
                dumper.run("--connector", connector.getName(), "-Dhiveql.rpc.protection=privacy"));

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
    dumper.run("--connector", connector.getName(), "-Dtest.test.property=test-value");

    // Assert
    assertTrue(file.exists());
  }
}
