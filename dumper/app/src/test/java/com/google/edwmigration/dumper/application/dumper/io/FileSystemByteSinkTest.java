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
package com.google.edwmigration.dumper.application.dumper.io;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.file.PathUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSystemByteSinkTest extends TestCase {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void writeMode_createTruncate() throws IOException {
    Path testPath = Paths.get(tempFolder.getRoot().toURI()).resolve("test-output.txt");
    FileSystemByteSink sink = new FileSystemByteSink(testPath, WriteMode.CREATE_TRUNCATE);

    // Write twice
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("first", outputStream, UTF_8);
    }
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("second", outputStream, UTF_8);
    }

    // Output contains only the content of the last write
    Assert.assertEquals("second", PathUtils.readString(testPath, UTF_8));

    // Write a third time using a new sink
    sink = new FileSystemByteSink(testPath, WriteMode.CREATE_TRUNCATE);
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("third", outputStream, UTF_8);
    }

    // Output contains only the content of the last write
    Assert.assertEquals("third", PathUtils.readString(testPath, UTF_8));
  }

  @Test
  public void writeMode_appendExisting() throws IOException {
    Path testPath = Paths.get(tempFolder.getRoot().toURI()).resolve("test-output.txt");

    // Write with CREATE_TRUNCATE first
    FileSystemByteSink sink = new FileSystemByteSink(testPath, WriteMode.CREATE_TRUNCATE);
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("first", outputStream, UTF_8);
    }

    // Then write twice with APPEND_EXISTING
    sink = new FileSystemByteSink(testPath, WriteMode.APPEND_EXISTING);
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("second", outputStream, UTF_8);
    }
    try (OutputStream outputStream = sink.openStream()) {
      IOUtils.write("third", outputStream, UTF_8);
    }

    // Output contains all writes appended.
    Assert.assertEquals("firstsecondthird", PathUtils.readString(testPath, UTF_8));
  }

  @Test
  public void writeMode_appendExisting_failNotExisting() {
    Path testPath = Paths.get(tempFolder.getRoot().toURI()).resolve("test-output.txt");

    // Append to not existing file
    FileSystemByteSink sink = new FileSystemByteSink(testPath, WriteMode.APPEND_EXISTING);
    // Writing will fail as no file exists to append to
    Assert.assertThrows(
        NoSuchFileException.class,
        () -> {
          try (OutputStream outputStream = sink.openStream()) {
            IOUtils.write("second", outputStream, UTF_8);
          }
        });
  }
}
