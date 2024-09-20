package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TeradataTablesValidatorTaskTest {
    @Mock private TaskRunContext context;

    @Mock private JdbcHandle jdbcHandle;

    @Mock private ByteSink sink;

    @Mock private Connection connection;

    @Mock private JdbcTemplate jdbcTemplate;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void tablesExists() throws Exception {
        TeradataTablesValidatorTask task = new TeradataTablesValidatorTask("x", "y", "z");

        when(jdbcHandle.getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(jdbcTemplate.queryForRowSet(anyString())).thenReturn(null); // no exception is ok

        task.doInConnection(context, jdbcHandle, sink, connection);

        verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from x"));
        verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from y"));
        verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from z"));
    }

    @Test
    public void someTablesDoNotExist() throws Exception {
        expectedException.expect(MetadataDumperUsageException.class);
        expectedException.expectMessage("The tables [y] do not exists or are not accessible.");

        TeradataTablesValidatorTask task = new TeradataTablesValidatorTask("x", "y", "z");

        when(jdbcHandle.getJdbcTemplate()).thenReturn(jdbcTemplate);
        when(jdbcTemplate.queryForRowSet(eq("select top 1 1 from x"))).thenReturn(null); // no exception is ok
        when(jdbcTemplate.queryForRowSet(eq("select top 1 1 from y"))).thenThrow(BadSqlGrammarException.class);
        when(jdbcTemplate.queryForRowSet(eq("select top 1 1 from z"))).thenReturn(null); // no exception is ok

        try {
            task.doInConnection(context, jdbcHandle, sink, connection);

            Assert.fail("Exception is expected");
        } catch (Exception e) {
            //verify that all the tables were validated
            //even with an exception for any
            verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from x"));
            verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from y"));
            verify(jdbcTemplate).queryForRowSet(eq("select top 1 1 from z"));

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
