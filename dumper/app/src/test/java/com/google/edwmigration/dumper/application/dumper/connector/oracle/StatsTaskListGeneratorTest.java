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

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StatsTaskListGeneratorTest {

  private final StatsTaskListGenerator generator = new StatsTaskListGenerator();

  // This is more of a configuration check than a unit test for app logic.
  // Still, it's convenient to include it in Gradle test.
  @Test
  public void nativeNames_returnsValidNames() throws IOException {
    ImmutableList<String> names = generator.nativeNames();
    ArrayList<String> failed = new ArrayList<>(names.size());
    // Act
    for (String item : names) {
      try {
        OracleStatsQuery.create(item, NATIVE);
      } catch (IllegalArgumentException e) {
        failed.add(item);
      }
    }
    // Assert
    assertEquals(ImmutableList.of(), failed);
  }
}
