package com.google.edwmigration.validation.application.validator.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Field;
import com.google.edwmigration.validation.application.validator.NameManager;
import com.google.edwmigration.validation.application.validator.ValidationArguments;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** @author nehanene */
@RunWith(MockitoJUnitRunner.class)
public class ComparisonTaskTest {
  @Mock
  private ValidationArguments mockArgs;
  @Mock
  private NameManager mockNameManager;

  @Test
  public void testConstructor_ThreePartTableId() {
    String bqResultsTable = "my-project.my_dataset.my_table";
    when(mockArgs.getBqResultsTable()).thenReturn(bqResultsTable);

    ComparisonTask task = new ComparisonTask(mockArgs, mockNameManager);

    assertEquals("my-project", task.getResultProject());
    assertEquals("my_dataset", task.getResultDataset());
    assertEquals("my_table", task.getResultTable());
    assertEquals(mockArgs, task.getArguments());
    assertEquals(mockNameManager, task.getNameManager());
  }

  @Test
  public void testConstructor_TwoPartTableId() {
    String bqResultsTable = "my_dataset.my_table";
    when(mockArgs.getBqResultsTable()).thenReturn(bqResultsTable);

    ComparisonTask task = new ComparisonTask(mockArgs, mockNameManager);

    assertNull("Project should be null for two-part ID", task.getResultProject());
    assertEquals("my_dataset", task.getResultDataset());
    assertEquals("my_table", task.getResultTable());
    assertEquals(mockArgs, task.getArguments());
    assertEquals(mockNameManager, task.getNameManager());
  }

  @Test
  public void testConstructor_InvalidTableId_TooFewParts() {
    String bqResultsTable = "my_table"; // Only one part
    when(mockArgs.getBqResultsTable()).thenReturn(bqResultsTable);

    try {
      new ComparisonTask(mockArgs, mockNameManager);
      fail("IllegalArgumentException was expected but not thrown for too few table ID parts.");
    } catch (IllegalArgumentException e) {
      String expectedMessagePart = "Invalid BQ result table ID. Please provide `project.dataset.table`: " + bqResultsTable;
      assertTrue("Exception message should indicate invalid ID format", e.getMessage().contains(expectedMessagePart));
    }
  }

  @Test
  public void testConstructor_InvalidTableId_TooManyParts() {
    String bqResultsTable = "too.many.parts.here"; // More than three parts
    when(mockArgs.getBqResultsTable()).thenReturn(bqResultsTable);

    try {
      new ComparisonTask(mockArgs, mockNameManager);
      fail("IllegalArgumentException was expected but not thrown for too many table ID parts.");
    } catch (IllegalArgumentException e) {
      String expectedMessagePart = "Invalid BQ result table ID. Please provide `project.dataset.table`: " + bqResultsTable;
      assertTrue("Exception message should indicate invalid ID format", e.getMessage().contains(expectedMessagePart));
    }
  }

  @Test
  public void testLoadSchemaFromJson_Success() {
    String bqResultsTable = "my_dataset.my_table";
    when(mockArgs.getBqResultsTable()).thenReturn(bqResultsTable);
    ComparisonTask task = new ComparisonTask(mockArgs, mockNameManager); // Constructor needs these

    Schema schema = task.loadSchemaFromJson();

    assertNotNull("Schema should not be null", schema);

    List<Field> fields = schema.getFields();
    assertNotNull("Fields list should not be null", fields);
    assertEquals("Expected 12 fields in the schema", 12, fields.size());

    Field field1 = fields.get(0);
    assertEquals("First field name should be 'run_id'", "run_id", field1.getName());
    assertEquals("First field type should be STRING", LegacySQLTypeName.STRING, field1.getType());

    Field field2 = fields.get(1);
    assertEquals("Second field name should be 'start_time'", "start_time", field2.getName());
    assertEquals("Second field type should be TIMESTAMP", LegacySQLTypeName.TIMESTAMP, field2.getType());

    Field field11 = fields.get(11);
    assertEquals("Fourth field name should be 'stddev_differences'", "stddev_differences", field11.getName());
    assertEquals("Fourth field type should be NUMERIC", LegacySQLTypeName.NUMERIC, field11.getType());
  }


}
