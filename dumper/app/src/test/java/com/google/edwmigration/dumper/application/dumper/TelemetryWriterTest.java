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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.FileSystems.newFileSystem;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.readAllLines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.metrics.ClientTelemetry;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TelemetryWriterTest {

  private static final String CACHE_FILE_NAME = "all-dumper-telemetry.jsonl";
  private static final String ZIP_ENTRY_NAME = "dumper-telemetry.jsonl";

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void write_secondRun_zipHoldsOnlyCurrentRunRecord() throws IOException {
    Path cacheDir = temporaryFolder.newFolder("cache").toPath();

    ImmutableList<String> firstZipLines = writeRunToNewZip(cacheDir, "first.zip");
    ImmutableList<String> secondZipLines = writeRunToNewZip(cacheDir, "second.zip");

    assertEquals(1, firstZipLines.size());
    assertEquals(1, secondZipLines.size());
    assertNotEquals(firstZipLines, secondZipLines);
  }

  @Test
  public void write_twoRuns_appendsBothRecordsToCacheFileInsideCacheDir() throws IOException {
    Path cacheDir = temporaryFolder.newFolder("cache").toPath();

    ImmutableList<String> firstZipLines = writeRunToNewZip(cacheDir, "first.zip");
    ImmutableList<String> secondZipLines = writeRunToNewZip(cacheDir, "second.zip");

    List<String> cachedLines = readAllLines(cacheDir.resolve(CACHE_FILE_NAME), UTF_8);
    ImmutableList<String> expectedLines =
        ImmutableList.<String>builder().addAll(firstZipLines).addAll(secondZipLines).build();
    assertEquals(expectedLines, cachedLines);
    assertFalse(exists(cacheDir.resolveSibling(cacheDir.getFileName() + CACHE_FILE_NAME)));
  }

  private ImmutableList<String> writeRunToNewZip(Path cacheDir, String zipName) throws IOException {
    Path zipPath = temporaryFolder.getRoot().toPath().resolve(zipName);
    URI zipUri = URI.create("jar:" + zipPath.toUri());
    try (FileSystem zipFileSystem = newFileSystem(zipUri, ImmutableMap.of("create", "true"))) {
      TelemetryWriter.write(zipFileSystem, new ClientTelemetry(), cacheDir);
      return ImmutableList.copyOf(readAllLines(zipFileSystem.getPath(ZIP_ENTRY_NAME), UTF_8));
    }
  }
}
