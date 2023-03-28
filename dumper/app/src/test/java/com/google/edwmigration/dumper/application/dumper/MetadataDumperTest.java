/*
 * Copyright 2022-2023 Google LLC
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

import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.bigquery.BigQueryLogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.hive.HiveMetadataConnector;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class MetadataDumperTest {

  // TODO(ishmum): `testOverridesZipWithGivenName` with content check
  private File file;
  private MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
  private final Connector connector = new HiveMetadataConnector();
  private final String defaultFileName = "dwh-migration-hiveql-metadata.zip";

  @After
  public void tearDown() throws IOException {
    if (file == null || !file.exists()) return;
    if (file.isDirectory()) FileUtils.deleteDirectory(file);
    else file.delete();
  }

  @Test
  public void testInstantiate() throws Exception {
    dumper = new MetadataDumper();
    dumper.run("--connector", new BigQueryLogsConnector().getName(), "--dry-run");
  }

  @Test
  public void testCreatesDefaultOutputZip() throws Exception {
    // Arrange
    file = new File(defaultFileName);

    // Act
    dumper.run("--connector", connector.getName());

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testCreatesDefaultOutputZipInProvidedDirectory() throws Exception {
    // Arrange
    String path = "dir";
    file = new File(path, defaultFileName);

    // Act
    dumper.run("--connector", connector.getName(), "--output", path);

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testCreatesZipWithGivenName() throws Exception {
    // Arrange
    String name = "test.zip";
    file = new File(name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testOverridesZipWithDefaultName() throws Exception {
    // Arrange
    File expectedFile = new File("test-dir/" + defaultFileName);
    String name = "test-dir";
    file = new File(name);

    // Act
    file.mkdirs();
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(expectedFile.exists());
  }

  @Test
  public void testCreatesFileInsideFolderNameWithZip() throws Exception {
    // Arrange
    File expectedFile = new File("dir.zip/" + defaultFileName);
    String name = "dir.zip";
    file = new File(name);

    // Act
    file.mkdirs();
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(expectedFile.exists());
  }

  @Test
  public void testDoesNotOverrideFileWithDirectory() throws IOException {
    // Arrange
    String name = "test";
    file = new File(name);

    // Act
    file.createNewFile();
    IllegalStateException exception =
        Assert.assertThrows(
            "No exception thrown from " + dumper.getClass().getSimpleName(),
            IllegalStateException.class,
            () -> dumper.run("--connector", connector.getName(), "--output", name));

    // Assert
    Assert.assertTrue(exception.getMessage().startsWith("A file already exists at test"));
  }
}
