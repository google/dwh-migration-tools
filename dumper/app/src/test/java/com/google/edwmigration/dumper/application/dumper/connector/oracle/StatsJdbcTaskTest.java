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
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import org.junit.Test;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class StatsJdbcTaskTest {

  enum ResultProperty {
    ACTION("Write to"),
    DESTINATION("pdbs-info.csv"),
    INPUT_NAME("name=pdbs-info"),
    STATS_SOURCE("statsSource=NATIVE");

    final String value;

    ResultProperty(String value) {
      this.value = value;
    }
  }

  @Theory
  public void toString_success(ResultProperty property) throws IOException {
    OracleStatsQuery query = OracleStatsQuery.create("pdbs-info", NATIVE, 30);
    Task<?> task = StatsJdbcTask.fromQuery(query);

    // Act
    String taskString = task.toString();

    // Assert
    assertTrue(taskString, taskString.contains(property.value));
  }

  @Test
  public void getCategory_success() throws IOException {
    OracleStatsQuery query = OracleStatsQuery.create("pdbs-info", NATIVE, 30);
    Task<?> task = StatsJdbcTask.fromQuery(query);

    assertEquals(TaskCategory.REQUIRED, task.getCategory());
  }
}
