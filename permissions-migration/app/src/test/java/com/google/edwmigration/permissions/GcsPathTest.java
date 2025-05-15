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
package com.google.edwmigration.permissions;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

public class GcsPathTest {
  @Test
  public void parse_simplePath_success() {
    GcsPath gcsPath = GcsPath.parse("gs://my-bucket/file.txt");

    GcsPath expectedPath = GcsPath.create("my-bucket", "file.txt");
    assertThat(gcsPath).isEqualTo(expectedPath);
  }

  @Test
  public void parse_nestedPath_success() {
    GcsPath gcsPath = GcsPath.parse("gs://my-bucket/folder-1/folder-2/file.txt");

    GcsPath expectedPath = GcsPath.create("my-bucket", "folder-1/folder-2/file.txt");
    assertThat(gcsPath).isEqualTo(expectedPath);
  }

  @Test
  public void parse_invalidPath_throwsException() {
    assertThrows(
        "IllegalArgumentException was expected but not thrown",
        IllegalArgumentException.class,
        () -> GcsPath.parse("not-a-valid-GCS-path"));
  }

  @Test
  public void toString_success() {
    GcsPath gcsPath = GcsPath.create("my-bucket", "folder-1/folder-2/file.txt");

    String result = gcsPath.toString();

    String expectedString = "gs://my-bucket/folder-1/folder-2/file.txt";
    assertThat(result).isEqualTo(expectedString);
  }

  @Test
  public void normalizePathSuffix_pathEndsWithSlash_returnsSameObject() {
    GcsPath gcsPath = GcsPath.create("bucket", "folder/");

    GcsPath result = gcsPath.normalizePathSuffix();

    assertSame(gcsPath, result);
  }

  @Test
  public void normalizePathSuffix_pathDoesNotEndWithSlash_slashIsAdded() {
    GcsPath gcsPath = GcsPath.create("bucket", "folder");

    GcsPath result = gcsPath.normalizePathSuffix();

    assertThat(result.bucketName()).isEqualTo("bucket");
    assertThat(result.objectName()).isEqualTo("folder/");
  }
}
