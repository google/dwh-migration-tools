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

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleConnectorScope.LOGS;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleConnectorScope.METADATA;
import static com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleConnectorScope.STATS;
import static java.time.ZoneOffset.UTC;
import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.time.Clock;
import java.time.Instant;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class OracleConnectorScopeTest {
  Instant instant = Instant.ofEpochMilli(1715346130945L);
  Clock clock = Clock.fixed(instant, UTC);

  @Test
  public void connectorName_success() {
    assertEquals("oracle-logs", LOGS.connectorName());
    assertEquals("oracle", METADATA.connectorName());
    assertEquals("oracle-stats", STATS.connectorName());
  }

  @Test
  public void formatName_success() {
    assertEquals("oracle.logs.zip", LOGS.formatName());
    assertEquals("oracle.dump.zip", METADATA.formatName());
    assertEquals("oracle.stats.zip", STATS.formatName());
  }

  @DataPoints("assessment")
  public static ImmutableList<TestCase> ASSESSMENT_TEST_CASES =
      ImmutableList.of(
          TestCase.create(LOGS, "dwh-migration-oracle-logs-20240510T130210.zip"),
          TestCase.create(METADATA, "dwh-migration-oracle-metadata.zip"),
          TestCase.create(STATS, "dwh-migration-oracle-stats.zip"));

  @Theory
  public void toFileName_forAssessment_success(@FromDataPoints("assessment") TestCase testCase) {
    String filename = testCase.oracleConnectorScope().toFileName(/* isAssessment= */ true, clock);
    assertEquals(testCase.expectedFilename(), filename);
  }

  @DataPoints("nonAssessment")
  public static ImmutableList<TestCase> NON_ASSESSMENT_TEST_CASES =
      ImmutableList.of(
          TestCase.create(LOGS, "dwh-migration-oracle-logs.zip"),
          TestCase.create(METADATA, "dwh-migration-oracle-metadata.zip"),
          TestCase.create(STATS, "dwh-migration-oracle-stats.zip"));

  @Theory
  public void toFileName_notForAssessment_success(
      @FromDataPoints("nonAssessment") TestCase testCase) {
    String filename = testCase.oracleConnectorScope().toFileName(/* isAssessment= */ false, clock);
    assertEquals(testCase.expectedFilename(), filename);
  }

  @AutoValue
  abstract static class TestCase {
    abstract OracleConnectorScope oracleConnectorScope();

    abstract String expectedFilename();

    static TestCase create(OracleConnectorScope oracleConnectorScope, String expectedFilename) {
      return new AutoValue_OracleConnectorScopeTest_TestCase(
          oracleConnectorScope, expectedFilename);
    }
  }
}
