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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LocalFilesystemScanCommandGeneratorTest {

  private static final String ROOT_SCAN_PREFIX = "find / ";

  private static final Path MEMORY_FILESYSTEM_DIR = Paths.get("/dev/shm");

  private static final String MEMORY_FILESYSTEM_TYPE = "tmpfs";

  private static final long SCAN_TIMEOUT_SECONDS = 30;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final List<Path> createdDirs = new ArrayList<>();

  @After
  public void tearDown() throws IOException {
    for (Path createdDir : createdDirs) {
      FileUtils.deleteDirectory(createdDir.toFile());
    }
  }

  @Test
  public void generate_success() {
    String command = LocalFilesystemScanCommandGenerator.generate();
    assertEquals(
        "find / -type d ! \\( -fstype ext2 -o -fstype ext3 -o -fstype ext4 -o -fstype xfs -o -fstype btrfs -o -fstype zfs -o -fstype jfs -o -fstype reiserfs -o -fstype f2fs -o -fstype overlay \\) -prune -o \\( -iname 'phoenix*.jar' -o -iname '*coprocessor*.jar' -o -iname '*jdbc*.jar' -o -iname '*odbc*.jar' -o -iname 'salesforce' -o -iname 'ngdbc.jar' -o -iname '*connector*.jar' -o -iname 'oozie-site.xml' -o -iname 'splunk' -o -iname 'newrelic-infra.yml' -o -iname 'elasticsearch.yml' -o -iname 'ganglia.conf' \\) -print 2>/dev/null",
        command);
  }

  @Test
  public void generate_directoryOnDiskFilesystem_listsMatchingNames() throws Exception {
    Path startDir = createDirOnScannedFilesystem();
    assumeTrue(startDir != null);
    Path libDir = Files.createDirectory(startDir.resolve("lib"));
    Path splunkDir = Files.createDirectory(startDir.resolve("splunk"));
    Path phoenixJar = Files.createFile(libDir.resolve("phoenix-core.jar"));
    Path jdbcJar = Files.createFile(libDir.resolve("OJDBC8.JAR"));
    Path oozieSite = Files.createFile(startDir.resolve("oozie-site.xml"));
    Files.createFile(libDir.resolve("notes.txt"));

    ImmutableSet<String> listing = scan(startDir);

    assertEquals(
        ImmutableSet.of(
            splunkDir.toString(), phoenixJar.toString(), jdbcJar.toString(), oozieSite.toString()),
        listing);
  }

  @Test
  public void generate_directoryOnMemoryFilesystem_listsNothing() throws Exception {
    assumeTrue(Files.isDirectory(MEMORY_FILESYSTEM_DIR) && Files.isWritable(MEMORY_FILESYSTEM_DIR));
    assumeTrue(MEMORY_FILESYSTEM_TYPE.equals(Files.getFileStore(MEMORY_FILESYSTEM_DIR).type()));
    Path startDir = createDir(MEMORY_FILESYSTEM_DIR);
    Files.createFile(startDir.resolve("phoenix-core.jar"));
    Files.createDirectory(startDir.resolve("splunk"));

    ImmutableSet<String> listing = scan(startDir);

    assertEquals(ImmutableSet.of(), listing);
  }

  /**
   * Creates a directory on a filesystem that the generated command scans: in the temporary
   * directory or, if that is on a memory filesystem, in the build directory. Returns null if
   * neither is on such a filesystem.
   */
  private Path createDirOnScannedFilesystem() throws IOException {
    ImmutableList<Path> parentDirs =
        ImmutableList.of(temporaryFolder.getRoot().toPath(), Paths.get("build").toAbsolutePath());
    for (Path parentDir : parentDirs) {
      if (Files.isDirectory(parentDir) && isScanned(Files.getFileStore(parentDir).type())) {
        return createDir(parentDir);
      }
    }

    return null;
  }

  private Path createDir(Path parentDir) throws IOException {
    Path createdDir = Files.createTempDirectory(parentDir, "local-filesystem-scan");
    createdDirs.add(createdDir);
    return createdDir;
  }

  /** Returns whether the generated command descends into filesystems of the given type. */
  private static boolean isScanned(String filesystemType) {
    return LocalFilesystemScanCommandGenerator.generate()
        .contains(" -fstype " + filesystemType + " ");
  }

  /** Runs the generated command with the given directory in place of the root directory. */
  private ImmutableSet<String> scan(Path startDir) throws Exception {
    String command = LocalFilesystemScanCommandGenerator.generate();
    assertTrue(command.startsWith(ROOT_SCAN_PREFIX));
    String scopedCommand =
        String.format("find '%s' %s", startDir, command.substring(ROOT_SCAN_PREFIX.length()));
    File output = temporaryFolder.newFile();
    Process process =
        new ProcessBuilder("/bin/bash", "-c", scopedCommand).redirectOutput(output).start();

    assertTrue("Scan did not finish.", process.waitFor(SCAN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    return ImmutableSet.copyOf(Files.readAllLines(output.toPath(), UTF_8));
  }
}
