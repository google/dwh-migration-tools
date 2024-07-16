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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.io.IOException;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class OracleStatsConnectorTest {

  @Test
  public void getConnectorScope_success() {
    OracleStatsConnector connector = new OracleStatsConnector();
    assertEquals(OracleConnectorScope.STATS, connector.getConnectorScope());
  }

  @Theory
  public void getQueryLogDays_success(ValidLogTime time) throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "oracle-stats", "--query-log-days", String.valueOf(time.asDays));

    int days = OracleStatsConnector.getQueryLogDays(arguments);

    assertEquals(time.asDays, days);
  }

  @Theory
  public void getQueryLogDays_invalidValue_throwsException(InvalidLogTime time) throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", "oracle-stats", "--query-log-days", time.asDays);

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> OracleStatsConnector.getQueryLogDays(arguments));

    assertTrue(exception.getMessage().contains(time.asDays));
    assertTrue(exception.getMessage().startsWith("The number of days must be positive and not greater than"));
  }

  enum InvalidLogTime {
    NEGATIVE("-5"),
    ZERO("0"),
    EXCEEDS_MAX("999999");

    final String asDays;

    InvalidLogTime(String asDays) {
      this.asDays = asDays;
    }
  }

  enum ValidLogTime {
    MIN(1),
    SHORT(7),
    LONG(30),
    MAX(OracleStatsConnector.MAX_DAYS);

    final int asDays;

    ValidLogTime(int asDays) {
      this.asDays = asDays;
    }
  }
}
