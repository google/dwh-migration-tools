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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

import com.google.common.base.Splitter;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;

public class LocalFilesystemScanCommandGenerator {
  private static final Escaper SIMPLE_BASH_ESCAPER =
      Escapers.builder().addEscape('\'', "\\'").addEscape('\\', "\\\\").build();

  public static String generate() {
    try {
      URL resourceUrl = Resources.getResource("hadoop-extraction/search-expressions.txt");
      return "find / "
          + Splitter.on('\n')
              .trimResults()
              .omitEmptyStrings()
              .splitToStream(Resources.toString(resourceUrl, UTF_8))
              .map(searchExpression -> "-iname " + quote(searchExpression))
              .collect(joining(" -o "))
          + " 2>/dev/null";
    } catch (IOException e) {
      throw new IllegalStateException("Error generating local filesystem scan task.", e);
    }
  }

  private static String quote(String expression) {
    return String.format("'%s'", SIMPLE_BASH_ESCAPER.escape(expression));
  }
}
