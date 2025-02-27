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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class StatsTaskListGeneratorTest {

  static final StatsTaskListGenerator generator = new StatsTaskListGenerator();

  @DataPoints("awrNames")
  public static final ImmutableList<String> awrNames = generator.awrNames();

  @DataPoints("statspackNames")
  public static final ImmutableList<String> statspackNames = generator.statspackNames();

  @Theory
  public void awrNames_allNamedFilesExist(@FromDataPoints("awrNames") String name)
      throws IOException {
    OracleStatsQuery.createAwr(name, Duration.ofDays(7));
  }

  @Theory
  public void statspackNames_allNamedFilesExist(@FromDataPoints("statspackNames") String name)
      throws IOException {
    OracleStatsQuery.createStatspack(name, Duration.ofDays(7));
  }
}
