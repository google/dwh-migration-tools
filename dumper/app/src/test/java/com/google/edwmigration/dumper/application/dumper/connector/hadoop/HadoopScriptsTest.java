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
import static org.junit.Assert.assertTrue;

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
}
