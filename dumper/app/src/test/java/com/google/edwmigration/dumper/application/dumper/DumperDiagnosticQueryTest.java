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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DumperDiagnosticQueryTest {

  private static final String SECRET = "S3cr3t-Value-For-Test";

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void call_commandLineHoldsPassword_passwordNotReported() {
    System.setProperty(
        "sun.java.command",
        "com.google.edwmigration.dumper.application.dumper.Main --connector teradata --password "
            + SECRET);

    String report = createReport();

    assertFalse(report.contains(SECRET));
    assertFalse(report.contains("sun.java.command"));
  }

  @Test
  public void call_userDefinedSystemProperty_valueNotReported() {
    System.setProperty("javax.net.ssl.keyStorePassword", SECRET);

    String report = createReport();

    assertFalse(report.contains(SECRET));
    assertFalse(report.contains("javax.net.ssl.keyStorePassword"));
  }

  @Test
  public void call_reportsJvmAndOperatingSystem() {
    String report = createReport();

    assertTrue(report.contains("java.version: " + System.getProperty("java.version")));
    assertTrue(report.contains("os.name: " + System.getProperty("os.name")));
  }

  @Test
  public void call_reportsThrowable() {
    String report = createReport();

    assertTrue(report.contains("Simulated task failure"));
  }

  private static String createReport() {
    Exception exception = new IllegalStateException("Simulated task failure");
    return String.valueOf(new DumperDiagnosticQuery(exception).call());
  }
}
