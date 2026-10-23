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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSystemOutputHandleTest {

  private static final String TARGET_PATH = "columns.csv";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void commit_replacesTargetCommittedByEarlierRun() throws IOException {
    // The dumper writes its output to a zip file system, which --continue opens again.
    URI uri = URI.create("jar:" + temporaryFolder.getRoot().toPath().resolve("out.zip").toUri());
    try (FileSystem zip = FileSystems.newFileSystem(uri, ImmutableMap.of("create", "true"))) {
      Path root = zip.getPath("/");
      Files.write(root.resolve(TARGET_PATH), "earlier run".getBytes(UTF_8));
      OutputHandle handle = new FileSystemOutputHandle(root, TARGET_PATH);
      handle.asTemporaryByteSink().write("this run".getBytes(UTF_8));

      handle.commit();

      assertEquals("this run", new String(Files.readAllBytes(root.resolve(TARGET_PATH)), UTF_8));
      assertFalse(Files.exists(root.resolve(TARGET_PATH + ".tmp")));
    }
  }
}
