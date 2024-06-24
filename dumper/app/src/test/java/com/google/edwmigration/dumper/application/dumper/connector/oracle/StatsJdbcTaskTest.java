package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.edwmigration.dumper.application.dumper.connector.oracle.StatsTaskListGenerator.StatsSource.NATIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;

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
    OracleStatsQuery query = OracleStatsQuery.create("pdbs-info", NATIVE);
    Task<?> task = StatsJdbcTask.fromQuery(query);

    // Act
    String taskString = task.toString();

    // Assert
    assertTrue(taskString, taskString.contains(property.value));
  }

  @Theory
  public void getCategory_success() throws IOException {
    OracleStatsQuery query = OracleStatsQuery.create("pdbs-info", NATIVE);
    Task<?> task = StatsJdbcTask.fromQuery(query);

    assertEquals(TaskCategory.REQUIRED, task.getCategory());
  }
}
