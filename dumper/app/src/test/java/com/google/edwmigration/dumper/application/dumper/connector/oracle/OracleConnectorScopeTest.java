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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleConnectorScope.*;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

import java.time.Clock;
import java.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleConnectorScopeTest {

  @Test
  public void toDisplayName_success() {
    assertEquals("oracle-logs", LOGS.connectorName());
    assertEquals("oracle", METADATA.connectorName());
    assertEquals("oracle-stats", STATS.connectorName());
  }

  @Test
  public void toFormat_success() {
    assertEquals("oracle.logs.zip", LOGS.formatName());
    assertEquals("oracle.dump.zip", METADATA.formatName());
    assertEquals("oracle.stats.zip", STATS.formatName());
  }

  @Test
  public void toFileName_forAssessment_success() {
    Instant instant = Instant.ofEpochMilli(1715346130945L);
    Clock clock = Clock.fixed(instant, UTC);

    assertEquals(
        "dwh-migration-oracle-logs-logs-20240510T130210.zip", LOGS.toFileName(true, clock));
    assertEquals("dwh-migration-oracle-metadata.zip", METADATA.toFileName(true, clock));
    assertEquals("dwh-migration-oracle-stats.zip", STATS.toFileName(true, clock));
  }

  @Test
  public void toFileName_notForAssessment_success() {
    Instant instant = Instant.ofEpochMilli(1715346130945L);
    Clock clock = Clock.fixed(instant, UTC);

    assertEquals("dwh-migration-oracle-logs-logs.zip", LOGS.toFileName(false, clock));
    assertEquals("dwh-migration-oracle-metadata.zip", METADATA.toFileName(false, clock));
    assertEquals("dwh-migration-oracle-stats.zip", STATS.toFileName(false, clock));
  }
}
