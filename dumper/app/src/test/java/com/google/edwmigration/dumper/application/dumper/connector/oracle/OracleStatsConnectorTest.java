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
import java.time.Duration;
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
            "--connector",
            "oracle-stats",
            "--query-log-days",
            String.valueOf(time.duration.toDays()));

    Duration days = OracleStatsConnector.getQueriedDuration(arguments);

    assertEquals(time.duration, days);
  }

  @Theory
  public void getQueryLogDays_invalidValue_throwsException(InvalidLogTime time) throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "oracle-stats",
            "--query-log-days",
            String.valueOf(time.duration.toDays()));

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> OracleStatsConnector.getQueriedDuration(arguments));

    assertTrue(exception.getMessage().contains(String.valueOf(time.duration.toDays())));
    assertTrue(
        exception
            .getMessage()
            .startsWith("The number of days must be positive and not greater than"));
  }

  enum InvalidLogTime {
    NEGATIVE(Duration.ofDays(-5)),
    ZERO(Duration.ofDays(0)),
    EXCEEDS_MAX(Duration.ofDays(999999));

    final Duration duration;

    InvalidLogTime(Duration duration) {
      this.duration = duration;
    }
  }

  enum ValidLogTime {
    MIN(Duration.ofDays(1)),
    SHORT(Duration.ofDays(7)),
    LONG(Duration.ofDays(30)),
    MAX(OracleStatsConnector.MAX_DURATION);

    final Duration duration;

    ValidLogTime(Duration duration) {
      this.duration = duration;
    }
  }
}
