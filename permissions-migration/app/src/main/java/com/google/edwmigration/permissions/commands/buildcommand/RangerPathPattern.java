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

import com.google.re2j.Pattern;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class RangerPathPattern {

  private final String pattern;

  private final boolean recursive;

  public RangerPathPattern(String pattern, boolean recursive) {
    this.pattern = pattern;
    this.recursive = recursive;
  }

  /** Convert a Ranger wildcard pattern into a compiled regex. */
  public Pattern compile() {
    ArrayList<String> patterns = new ArrayList<>();
    if (recursive) {
      if (pattern.endsWith("/")) {
        // A recursive pattern '/path/' is expanded as '/path/*'.
        patterns.add(pattern + "*");
      } else if (!pattern.endsWith("*")) {
        // A recursive pattern '/path' is expanded as '/path' and '/path/*'.
        patterns.add(pattern);
        patterns.add(pattern + "/*");
      } else {
        // A recursive pattern '/path*' is left as is.
        patterns.add(pattern);
      }
    } else {
      // Non-recursive pattern.
      patterns.add(pattern);
    }
    return Pattern.compile(
        patterns.stream()
            .map(this::convertWildcardPatternToRegex)
            .collect(Collectors.joining("|")));
  }

  private String convertWildcardPatternToRegex(String pattern) {
    StringBuilder sb = new StringBuilder("^");
    StringBuilder chunk = new StringBuilder();
    for (char c : pattern.toCharArray()) {
      if (c == '*' || c == '?') {
        if (chunk.length() > 0) {
          sb.append(Pattern.quote(chunk.toString()));
          chunk.setLength(0);
        }
        sb.append(c == '*' ? ".*" : ".");
      } else {
        chunk.append(c);
      }
    }
    if (chunk.length() > 0) {
      sb.append(Pattern.quote(chunk.toString()));
    }
    sb.append("$");
    return sb.toString();
  }
}
