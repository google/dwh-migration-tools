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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TeradataTablesValidatorTaskTest {
  @Mock private TaskRunContext context;

  @Mock private JdbcHandle jdbcHandle;

  @Mock private ByteSink sink;

  @Mock private Connection connection;

  @Mock private PreparedStatement statement;

  @Test
  public void tablesExists_success() throws Exception {
    TeradataTablesValidatorTask task = new TeradataTablesValidatorTask("x", "y", "z");

    when(connection.prepareStatement(anyString())).thenReturn(statement);

    task.doInConnection(context, jdbcHandle, sink, connection);

    verify(connection).prepareStatement(eq("select 1 from x"));
    verify(connection).prepareStatement(eq("select 1 from y"));
    verify(connection).prepareStatement(eq("select 1 from z"));

    verify(statement, times(3)).setMaxRows(1);
    verify(statement, times(3)).execute();
  }

  @Test
  public void someTablesDoNotExist_throwsUsageException() throws Exception {
    TeradataTablesValidatorTask task = new TeradataTablesValidatorTask("x", "y", "z");

    when(connection.prepareStatement(eq("select 1 from x"))).thenReturn(statement);
    when(connection.prepareStatement(eq("select 1 from y"))).thenThrow(SQLException.class);
    when(connection.prepareStatement(eq("select 1 from z"))).thenReturn(statement);

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> task.doInConnection(context, jdbcHandle, sink, connection));

    assertEquals(exception.getMessage(), "The tables [y] do not exists or are not accessible.");
    // make sure all tables have been checked,
    // even with an exception for any
    verify(connection).prepareStatement(eq("select 1 from x"));
    verify(connection).prepareStatement(eq("select 1 from y"));
    verify(connection).prepareStatement(eq("select 1 from z"));
  }

  @Test
  public void noTables_throwsException() {
    assertThrows(IllegalArgumentException.class, TeradataTablesValidatorTask::new);
  }

  @Test
  public void nullTables_throwsException() {
    assertThrows(
        NullPointerException.class,
        () -> new TeradataTablesValidatorTask((String[]) null)); // cast to avoid compiler warnings
  }
}
