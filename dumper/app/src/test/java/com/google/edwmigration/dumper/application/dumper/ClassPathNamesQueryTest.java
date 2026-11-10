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

import static org.junit.Assert.assertEquals;

import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClassPathNamesQueryTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void call_reportsFileNamesInClassPathOrder() {
    setClassPath("/opt/dumper/lib/app.jar", "/home/someuser/drivers/extra.jar");

    String report = String.valueOf(new ClassPathNamesQuery().call());

    assertEquals("0: app.jar\n1: extra.jar\n", report);
  }

  @Test
  public void call_sameNameInTwoDirectories_reportsBoth() {
    setClassPath("/opt/dumper/lib/guava.jar", "/opt/dumper/extra/guava.jar");

    String report = String.valueOf(new ClassPathNamesQuery().call());

    assertEquals("0: guava.jar\n1: guava.jar\n", report);
  }

  private static void setClassPath(String... entries) {
    System.setProperty("java.class.path", String.join(File.pathSeparator, entries));
  }
}
