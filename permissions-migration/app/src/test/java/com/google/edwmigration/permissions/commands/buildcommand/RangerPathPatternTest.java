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
package com.google.edwmigration.permissions.commands.buildcommand;

import static com.google.common.truth.Truth.assertThat;

import com.google.re2j.Pattern;
import org.junit.jupiter.api.Test;

public class RangerPathPatternTest {

  @Test
  public void run_compilesToRegexMatchingBasicPattern() {
    String path = "/path/folder/table";

    Pattern actual = new RangerPathPattern(path, false).compile();

    assertThat(actual.matches("/path/folder/table")).isTrue();
    assertThat(actual.matches("/path/folder/table/partition1")).isFalse();
    assertThat(actual.matches("/path/folder/table2")).isFalse();
  }

  @Test
  public void run_compilesToRegexMatchingRecursiveFolderPattern() {
    String path = "/path/folder/table";

    Pattern actual = new RangerPathPattern(path, true).compile();

    assertThat(actual.matches("/path/folder/table")).isTrue();
    assertThat(actual.matches("/path/folder/table/partition1")).isTrue();
    assertThat(actual.matches("/path/folder/table2")).isFalse();
  }

  @Test
  public void run_compilesToRegexMatchingWildcardPattern() {
    String path = "/path/folder/table*";

    Pattern actual = new RangerPathPattern(path, false).compile();

    assertThat(actual.matches("/path/folder/table")).isTrue();
    assertThat(actual.matches("/path/folder/table/partition1")).isTrue();
    assertThat(actual.matches("/path/folder/table2")).isTrue();
    assertThat(actual.matches("/path/folder2/table3")).isFalse();
  }

  @Test
  public void run_compilesToRegexMatchingWildcardRecursivePattern() {
    String path = "/path/folder/table*";

    Pattern actual = new RangerPathPattern(path, true).compile();

    assertThat(actual.matches("/path/folder/table")).isTrue();
    assertThat(actual.matches("/path/folder/table/partition1")).isTrue();
    assertThat(actual.matches("/path/folder/table2")).isTrue();
    assertThat(actual.matches("/path/folder2/table3")).isFalse();
  }
}
