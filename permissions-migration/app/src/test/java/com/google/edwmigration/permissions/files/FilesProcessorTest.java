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
package com.google.edwmigration.permissions.files;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

public class FilesProcessorTest {
  @Test
  public void processesFiles() {
    String filePath = Resources.getResource("fileprocessor/1.txt").getPath();

    String result =
        FileProcessor.apply(
            filePath, path -> new String(Files.readAllBytes(path), StandardCharsets.UTF_8));

    assertThat(result).isEqualTo("1-one");
  }

  @Test
  public void processesFilesInDirectories() {
    String filePath = Resources.getResource("fileprocessor").getPath();

    String result =
        FileProcessor.apply(
            filePath,
            path -> new String(Files.readAllBytes(path.resolve("1.txt")), StandardCharsets.UTF_8));

    assertThat(result).isEqualTo("1-one");
  }

  @Test
  public void processesFilesInArchives() {
    String filePath = Resources.getResource("fileprocessor/archive.zip").getPath();

    String result =
        FileProcessor.apply(
            filePath,
            path -> new String(Files.readAllBytes(path.resolve("1.txt")), StandardCharsets.UTF_8));

    assertThat(result).isEqualTo("1-one");
  }
}
