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
package com.google.edwmigration.permissions.models;

import com.google.cloud.bigquery.TableId;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

public class TableIdParser {

  // TODO: replace this with a better solution. Using GoogleSql's lexer could help here.
  //
  // Translation formats the ID as a <project, dataset, table> triplet joined with '.' as separator.
  // Each token can be subject to backtick quoting.
  private static final Pattern TRANSLATION_BQ_ID =
      Pattern.compile(
          "^(?:(?:`([^`]+)`)|([^`.]+))\\.(?:(?:`([^`]+)`)|([^`.]+))\\.(?:(?:`([^`]+)`)|([^`.]+))$");

  public static TableId parseTranslationId(String id) {
    Matcher matcher = TableIdParser.TRANSLATION_BQ_ID.matcher(id);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("Invalid BigQuery identifier '%s': expected projectId.dataset.table", id));
    }
    String projectId = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
    String dataset = matcher.group(3) != null ? matcher.group(3) : matcher.group(4);
    String table = matcher.group(5) != null ? matcher.group(5) : matcher.group(6);
    return TableId.of(projectId, dataset, table);
  }
}
