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
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class MetadataDumperTest {

  private File file;

  @After
  public void tearDown() {
    if (file != null && file.exists()) file.delete();
  }

  @Test
  public void testInstantiate() throws Exception {
    MetadataDumper dumper = new MetadataDumper();
    dumper.run("--connector", new BigQueryLogsConnector().getName(), "--dry-run");
  }

  @Test
  public void testCreatesDefaultOutputZip() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = connector.getDefaultFileName(false);
    file = new File(name);

    // Act
    dumper.run("--connector", connector.getName());

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testCreatesDefaultOutputZipInProvidedDirectory() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = connector.getDefaultFileName(false);
    file = new File("test/dir", name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", "test/dir");

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testCreatesZipWithGivenName() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = "test.zip";
    file = new File(name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testOverridesZipWithGivenName() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = "test-dir/test.zip";
    file = new File(name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testOverridesZipWithDefaultName() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = "test-dir";
    file = new File(name);

    // Act
    dumper.run("--connector", connector.getName(), "--output", name);

    // Assert
    Assert.assertTrue(file.exists());
  }

  @Test
  public void testDoesNotOverrideDirectoryEndingWithZip() throws Exception {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = "test-dir/dir.zip";
    File folder = new File(name);

    // Act & Assert
    IllegalStateException exception =
        Assert.assertThrows(
            "No exception thrown from " + dumper.getClass().getSimpleName(),
            IllegalStateException.class,
            () -> dumper.run("--connector", connector.getName(), "--output", name));

    // Assert
    Assert.assertTrue(
        exception.getMessage().startsWith("A folder already exists at test-dir/dir.zip"));
    Assert.assertTrue(folder.exists());
    Assert.assertTrue(folder.isDirectory());
  }

  @Test
  public void testDoesNotOverrideFileWithDirectory() {
    // Arrange
    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);
    Connector connector = new HiveMetadataConnector();
    String name = "test-dir/test";
    File file = new File(name);

    // Act & Assert
    IllegalStateException exception =
        Assert.assertThrows(
            "No exception thrown from " + dumper.getClass().getSimpleName(),
            IllegalStateException.class,
            () -> dumper.run("--connector", connector.getName(), "--output", name));

    // Assert
    Assert.assertTrue(exception.getMessage().startsWith("A file already exists at test-dir/test"));
    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.isFile());
  }
}
