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
package com.google.edwmigration.dumper.application.dumper.io;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.springframework.util.FileSystemUtils;

/** @author shevek */
@RunWith(JUnit4.class)
public class FileSystemOutputHandleFactoryTest {

  private void testSinkFactory(@Nonnull FileSystemOutputHandleFactory factory) throws IOException {

    {
      OutputHandle handle = factory.newOutputFileHandle("temporary");
      assertFalse("temporary exists before write", handle.exists());
      handle.asTemporaryByteSink().write(Ints.toByteArray(1234));
      assertFalse("temporary exists after write - before commit", handle.exists());
      handle.commit();
      assertTrue("temporary does not exist after commit", handle.exists());
    }

    {
      OutputHandle handle = factory.newOutputFileHandle("direct");
      assertFalse("direct exists before write", handle.exists());
      handle.asByteSink().write(Ints.toByteArray(1234));
      assertTrue("direct does not exist after commit", handle.exists());
    }
  }

  @Test
  public void testFileSystemFile() throws Exception {
    File root = TestUtils.newOutputDirectory("filesystem.dir");
    FileSystemUtils.deleteRecursively(root);
    root.mkdirs();
    testSinkFactory(
        new FileSystemOutputHandleFactory(FileSystems.getDefault(), root.getAbsolutePath()));
    assertTrue("Dir exists.", root.exists());
    assertTrue("Dir is a directory.", root.isDirectory());
  }

  @Test
  public void testFileSystemZip() throws Exception {
    File root = TestUtils.newOutputFile("filesystem.zip");
    if (root.exists()) root.delete();
    else com.google.common.io.Files.createParentDirs(root);
    root.delete();
    URI uri = URI.create("jar:" + root.toURI());
    try (FileSystem fs = FileSystems.newFileSystem(uri, ImmutableMap.of("create", "true"))) {
      testSinkFactory(new FileSystemOutputHandleFactory(fs, "/"));
    }
    assertTrue("ZIP exists.", root.exists());
    assertTrue("ZIP is a file.", root.isFile());
  }
}
