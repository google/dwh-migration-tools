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
package com.google.edwmigration.validation.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.edwmigration.validation.NameManager;
import com.google.edwmigration.validation.ValidationArguments;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** @author nehanene */
@RunWith(MockitoJUnitRunner.class)
public class ComparisonTaskTest {
  @Mock private ValidationArguments mockArgs;
  @Mock private NameManager mockNameManager;

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
      String expectedMessagePart =
          "Invalid BQ result table ID. Please provide `project.dataset.table`: " + bqResultsTable;
      assertTrue(
          "Exception message should indicate invalid ID format",
          e.getMessage().contains(expectedMessagePart));
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
      String expectedMessagePart =
          "Invalid BQ result table ID. Please provide `project.dataset.table`: " + bqResultsTable;
      assertTrue(
          "Exception message should indicate invalid ID format",
          e.getMessage().contains(expectedMessagePart));
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
    assertEquals(
        "Second field type should be TIMESTAMP", LegacySQLTypeName.TIMESTAMP, field2.getType());

    Field field11 = fields.get(11);
    assertEquals(
        "Fourth field name should be 'stddev_differences'",
        "stddev_differences",
        field11.getName());
    assertEquals(
        "Fourth field type should be NUMERIC", LegacySQLTypeName.NUMERIC, field11.getType());
  }
}
