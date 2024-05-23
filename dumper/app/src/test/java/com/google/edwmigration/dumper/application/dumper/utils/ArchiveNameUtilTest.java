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
package com.google.edwmigration.dumper.application.dumper.utils;

import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

import java.time.Clock;
import java.time.Instant;
import org.junit.Test;

public class ArchiveNameUtilTest {

  @Test
  public void getFileName_success() {
    String name = "snowflake-information-schema-logs";

    assertEquals(
        "dwh-migration-snowflake-information-schema-logs.zip", ArchiveNameUtil.getFileName(name));
  }

  @Test
  public void getFileNameWithTimestamp_success() {
    Instant instant = Instant.ofEpochMilli(1715346130945L);
    Clock clock = Clock.fixed(instant, UTC);
    String name = "snowflake-information-schema-logs";

    assertEquals(
        "dwh-migration-snowflake-information-schema-logs-20240510T130210.zip",
        ArchiveNameUtil.getFileNameWithTimestamp(name, clock));
  }
}
