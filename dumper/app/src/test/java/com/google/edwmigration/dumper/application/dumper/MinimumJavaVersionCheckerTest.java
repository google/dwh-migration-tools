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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MinimumJavaVersionCheckerTest {

  @Test
  public void check_java5_throwsException() {
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> MinimumJavaVersionChecker.check("1.5.0"));

    assertEquals(
        "Currently running Java version '1.5.0'. Dumper requires Java 8 or higher.",
        e.getMessage());
  }

  @Test
  public void check_java7_throwsException() {
    MetadataDumperUsageException e =
        assertThrows(
            MetadataDumperUsageException.class, () -> MinimumJavaVersionChecker.check("1.7.0"));

    assertEquals(
        "Currently running Java version '1.7.0'. Dumper requires Java 8 or higher.",
        e.getMessage());
  }

  @Test
  public void check_java8() {
    // test that no exceptions are thrown
    MinimumJavaVersionChecker.check("1.8.0_182");
  }

  @Test
  public void check_java9() {
    // test that no exceptions are thrown
    MinimumJavaVersionChecker.check("9.0.1+7");
  }

  @Test
  public void check_java17() {
    // test that no exceptions are thrown
    MinimumJavaVersionChecker.check("17.0.4.1");
  }
}
