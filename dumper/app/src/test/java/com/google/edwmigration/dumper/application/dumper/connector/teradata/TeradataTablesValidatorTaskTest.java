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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void tablesExists() throws Exception {
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
  public void someTablesDoNotExist() throws Exception {
    expectedException.expect(MetadataDumperUsageException.class);
    expectedException.expectMessage("The tables [y] do not exists or are not accessible.");

    TeradataTablesValidatorTask task = new TeradataTablesValidatorTask("x", "y", "z");

    when(connection.prepareStatement(eq("select 1 from x"))).thenReturn(statement);
    when(connection.prepareStatement(eq("select 1 from y"))).thenThrow(SQLException.class);
    when(connection.prepareStatement(eq("select 1 from z"))).thenReturn(statement);

    try {
      task.doInConnection(context, jdbcHandle, sink, connection);

      Assert.fail("Exception is expected");
    } catch (Exception e) {
      // make sure all tables have been checked,
      // even with an exception for any
      verify(connection).prepareStatement(eq("select 1 from x"));
      verify(connection).prepareStatement(eq("select 1 from y"));
      verify(connection).prepareStatement(eq("select 1 from z"));

      throw e;
    }
  }

  @Test
  public void noTables() {
    expectedException.expect(IllegalArgumentException.class);

    new TeradataTablesValidatorTask();
    Assert.fail("At least 1 table name must be provided to the task.");
  }

  @Test
  public void nullTables() {
    expectedException.expect(NullPointerException.class);

    new TeradataTablesValidatorTask(null);
    Assert.fail("At least 1 table name must be provided to the task.");
  }
}
