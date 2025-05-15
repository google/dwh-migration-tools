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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.TableId;
import org.junit.jupiter.api.Test;

public class TableIdParserTest {

  @Test
  public void parseTranslationId_parsesUnquotedIds() {
    TableId tableId = TableIdParser.parseTranslationId("project.dataset.table");

    assertThat(tableId).isEqualTo(TableId.of("project", "dataset", "table"));
  }

  @Test
  public void parseTranslationId_parsesQuotedIds() {
    TableId tableId = TableIdParser.parseTranslationId("`project-test`.dataset.table");

    assertThat(tableId).isEqualTo(TableId.of("project-test", "dataset", "table"));
  }
}
