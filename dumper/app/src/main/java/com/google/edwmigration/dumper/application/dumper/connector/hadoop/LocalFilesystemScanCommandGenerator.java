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

import static java.util.stream.Collectors.joining;

import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

public class LocalFilesystemScanCommandGenerator {
  private static final Escaper SIMPLE_BASH_ESCAPER =
      Escapers.builder().addEscape('\'', "\\'").addEscape('\\', "\\\\").build();

  private static final ImmutableList<String> SEARCH_EXPRESSIONS = ImmutableList.of(
      "phoenix*.jar",
           "*coprocessor*.jar",
           "*jdbc*.jar",
           "*odbc*.jar",
           "salesforce",
           "ngdbc.jar",
           "*connector*.jar",
           "oozie-site.xml",
           "splunk",
           "newrelic-infra.yml",
           "elasticsearch.yml",
           "ganglia.conf"
  );

  public static String generate() {
      return "find / "
          + SEARCH_EXPRESSIONS.stream()
              .map(searchExpression -> "-iname " + quote(searchExpression))
              .collect(joining(" -o "))
          + " 2>/dev/null";
  }

  private static String quote(String expression) {
    return String.format("'%s'", SIMPLE_BASH_ESCAPER.escape(expression));
  }
}
