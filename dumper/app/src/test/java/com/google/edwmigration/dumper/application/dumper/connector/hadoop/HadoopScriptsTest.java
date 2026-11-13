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
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopScriptsTest {
  private static final long PROBE_TIMEOUT_SECONDS = 60;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  Path extractedFile;

  @After
  public void tearDown() throws IOException {
    if ((extractedFile != null) && Files.exists(extractedFile)) {
      Files.delete(extractedFile);
    }
  }

  @Test
  public void read_nonEmpty_success() throws IOException {
    byte[] scriptContent = HadoopScripts.read("hadoop-version.sh");
    assertTrue(scriptContent.length > 0);
  }

  @Test
  public void extract_success() throws IOException {
    String scriptFilename = "hadoop-version.sh";

    // Act
    extractedFile = HadoopScripts.extract(scriptFilename);

    // Assert
    assertArrayEquals(HadoopScripts.read(scriptFilename), Files.readAllBytes(extractedFile));
  }

  @Test
  public void extractSingleLineScripts_allScriptsPresent() {
    ImmutableMap<String, Path> singleLineScripts = HadoopScripts.extractSingleLineScripts();
    assertEquals(
        ImmutableSet.of(
            "hive-execution-engine-beeline",
            "ip-address",
            "hive-concurrency",
            "os-release",
            "hive-txn-manager",
            "ls-var-log",
            "hive-concurrency-beeline",
            "ls-usr-local-lib",
            "hive-txn-manager-beeline",
            "hive-execution-engine",
            "timedatectl-status",
            "hadoop-checknative",
            "dmidecode-processor",
            "blkid",
            "hdfs-crypto-zones",
            "hdfs-replication",
            "hadoop-security-authorization",
            "hadoop-security-authentication",
            "hadoop-rpc-protection",
            "hadoop-ssl-enabled",
            "hadoop-ssl-require-client-cert",
            "hadoop-ssl-server",
            "hadoop-ssl-client"),
        singleLineScripts.keySet());
  }

  @Test
  public void extractSingleLineScripts_allScriptsStartWithShebangAndBlankLine() {
    HadoopScripts.extractSingleLineScripts()
        .forEach(
            (scriptName, scriptFile) -> {
              ImmutableList<String> lines = readAllLines(scriptFile);
              assertEquals("#!/bin/bash", lines.get(0));
              assertTrue(lines.get(1).isEmpty());
            });
  }

  @Test
  public void extractSingleLineScripts_allScriptsHaveThreeLines() {
    HadoopScripts.extractSingleLineScripts()
        .forEach(
            (scriptName, scriptFile) -> {
              ImmutableList<String> lines = readAllLines(scriptFile);
              assertEquals(3, lines.size());
            });
  }

  @Test
  public void extractSingleLineScripts_osRelease() {
    ImmutableList<String> osReleaseScript =
        readAllLines(HadoopScripts.extractSingleLineScripts().get("os-release"));
    assertEquals(ImmutableList.of("#!/bin/bash", "", "cat /etc/os-release"), osReleaseScript);
  }

  @Test
  public void scriptDir_isInSystemTemporaryDirectory() {
    Path systemTemporaryDir = Paths.get(System.getProperty("java.io.tmpdir")).toAbsolutePath();

    Path scriptDir = HadoopScripts.getScriptDir().toAbsolutePath();

    assertEquals(systemTemporaryDir, scriptDir.getParent());
  }

  @Test
  public void scriptDir_isAccessibleOnlyByOwner() throws IOException {
    assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));

    Path scriptDir = HadoopScripts.getScriptDir();

    assertEquals(
        ImmutableSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE),
        Files.getPosixFilePermissions(scriptDir));
  }

  @Test
  public void scriptDir_jvmExits_isDeleted() throws Exception {
    File workingDir = temporaryFolder.newFolder("working-dir");
    Path scriptDirRecord = temporaryFolder.getRoot().toPath().resolve("script-dir.txt");
    Path javaBinary = Paths.get(System.getProperty("java.home"), "bin", "java");
    Process probe =
        new ProcessBuilder(
                javaBinary.toString(),
                "-cp",
                System.getProperty("java.class.path"),
                HadoopScriptsExitProbe.class.getName(),
                scriptDirRecord.toString())
            .directory(workingDir)
            .redirectErrorStream(true)
            .redirectOutput(temporaryFolder.newFile("probe.log"))
            .start();

    assertTrue("Probe did not exit.", probe.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertEquals(0, probe.exitValue());
    Path scriptDir = Paths.get(new String(Files.readAllBytes(scriptDirRecord), UTF_8));
    assertFalse("Directory still exists: " + scriptDir, Files.exists(scriptDir));
    assertEquals(0, workingDir.list().length);
  }

  private static ImmutableList<String> readAllLines(Path path) {
    try {
      return ImmutableList.copyOf(Files.readAllLines(path));
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Error reading file '%s'.", path), e);
    }
  }
}
