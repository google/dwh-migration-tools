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
package com.google.edwmigration.dumper.application.dumper.utils;

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OptionalUtilsTest {

  @Test
  public void optionallyIfNotEmpty_null() {
    assertEquals(Optional.empty(), OptionalUtils.optionallyIfNotEmpty(null));
  }

  @Test
  public void optionallyIfNotEmpty_emptyString() {
    assertEquals(Optional.empty(), OptionalUtils.optionallyIfNotEmpty(""));
  }

  @Test
  public void optionallyIfNotEmpty_success() {
    assertEquals(Optional.of("abc"), OptionalUtils.optionallyIfNotEmpty("abc"));
  }

  @Test
  public void optionallyIfNotEmpty_space() {
    assertEquals(Optional.of(" "), OptionalUtils.optionallyIfNotEmpty(" "));
  }
}
