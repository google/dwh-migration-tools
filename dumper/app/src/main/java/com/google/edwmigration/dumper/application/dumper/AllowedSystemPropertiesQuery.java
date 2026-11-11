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

import com.google.common.collect.ImmutableList;
import org.anarres.jdiagnostics.AbstractQuery;
import org.anarres.jdiagnostics.Result;

/**
 * Reports a fixed set of system properties that describe the JVM and the operating system.
 *
 * <p>The report is written into the output archive, which leaves the machine the dumper ran on.
 * Reporting every system property would include {@code sun.java.command}, which holds the whole
 * command line with the passwords and keys passed as arguments, and any secret passed as a {@code
 * -D} property. Only the properties named here are reported.
 */
class AllowedSystemPropertiesQuery extends AbstractQuery {

  private static final ImmutableList<String> ALLOWED_PROPERTIES =
      ImmutableList.of(
          "java.version",
          "java.vendor",
          "java.runtime.name",
          "java.runtime.version",
          "java.vm.name",
          "java.vm.vendor",
          "java.vm.version",
          "java.specification.version",
          "java.class.version",
          "os.name",
          "os.arch",
          "os.version",
          "file.encoding",
          "sun.jnu.encoding",
          "user.language",
          "user.country",
          "user.timezone");

  @Override
  public String getName() {
    return "systemProperties";
  }

  @Override
  public void call(Result result, String prefix) {
    for (String name : ALLOWED_PROPERTIES) {
      String value = System.getProperty(name);
      if (value != null) {
        result.put(prefix + name, value);
      }
    }
  }
}
