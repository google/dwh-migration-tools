/*
 * Copyright 2022-2024 Google LLC
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopScriptsTest {
  Path extractedFile;

  @BeforeClass
  public static void setUp() {
    ScriptTmpDirCleanup.cleanupAfterAllTestsAreFinished();
  }

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
        ImmutableSet.of("ip-address", "os-release", "ls-var-log", "ls-usr-local-lib"),
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

  private static ImmutableList<String> readAllLines(Path path) {
    try {
      return ImmutableList.copyOf(Files.readAllLines(path));
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Error reading file '%s'.", path), e);
    }
  }
}
