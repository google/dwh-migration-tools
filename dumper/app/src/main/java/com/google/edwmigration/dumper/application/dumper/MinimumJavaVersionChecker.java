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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MinimumJavaVersionChecker {
  private static final Logger LOG = LoggerFactory.getLogger(MinimumJavaVersionChecker.class);

  static void check(String version) {
    if (!version.startsWith("1.")) {
      return;
    }
    Pattern oldVersionPattern = Pattern.compile("1\\.([0-9]+)");
    Matcher matcher = oldVersionPattern.matcher(version);
    if (matcher.find()) {
      String match = matcher.group(1);
      if (match.length() != 1) {
        LOG.warn("Unrecognized Java version '{}'.", version);
        return;
      }
      try {
        int majorVersion = Integer.parseInt(match);
        if (majorVersion < 8) {
          throw new MetadataDumperUsageException(
              String.format(
                  "Currently running Java version '%s'. Dumper requires Java 8 or higher.",
                  version));
        }
      } catch (NumberFormatException e) {
        LOG.warn("Unrecognized Java version '{}'.", version, e);
      }
    }
  }
}
