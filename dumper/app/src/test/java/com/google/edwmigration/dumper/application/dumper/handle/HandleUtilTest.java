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
package com.google.edwmigration.dumper.application.dumper.handle;

import static com.google.edwmigration.dumper.application.dumper.handle.HandleUtil.createHikariConfig;
import static com.google.edwmigration.dumper.application.dumper.handle.HandleUtil.verifyJdbcConnection;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils;
import com.zaxxer.hikari.HikariConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HandleUtilTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void verifyJdbcConnection_nullConnection_exceptionThrown() throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getConnection()).thenReturn(null);

    assertThrows(NullPointerException.class, () -> verifyJdbcConnection(dataSource));
  }

  @Test
  public void verifyJdbcConnection_validConnection_closesConnection() throws SQLException {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    when(dataSource.getConnection()).thenReturn(connection);

    verifyJdbcConnection(dataSource);

    verify(connection).close();
  }

  @Test
  public void withPoolConfig_success_poolSizeMatches() {
    DataSource dataSource = mock(DataSource.class);

    HikariConfig config = createHikariConfig(dataSource, 2);

    assertEquals(2, config.getMaximumPoolSize());
  }

  @Test
  public void withPoolConfig_success_timeoutEqualsIntMax() {
    DataSource dataSource = mock(DataSource.class);

    HikariConfig config = createHikariConfig(dataSource, 1);

    assertEquals(Integer.MAX_VALUE, config.getConnectionTimeout());
  }

  @Test
  public void jdbcHandle_setsDefaultFetchSizeOnQueries() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT 1")).thenReturn(resultSet);

    JdbcHandle handle = new JdbcHandle(dataSource);
    handle.executeQuery("SELECT 1");

    verify(statement).setFetchSize(1024);
  }

  @Test
  public void jdbcHandle_closeFails_queryStillSucceeds() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT 1")).thenReturn(resultSet);
    doThrow(new SQLException("close failed")).when(resultSet).close();
    doThrow(new SQLException("close failed")).when(statement).close();

    JdbcHandle handle = new JdbcHandle(dataSource);
    handle.executeQuery("SELECT 1");

    verify(statement).close();
    verify(connection, times(2)).close();
  }

  @Test
  public void jdbcHandle_executesScriptsAndQueriesAgainstSqlite() throws Exception {
    File dbFile = DumperTestUtils.newJdbcFile("handle_util_test");
    dbFile.delete();
    Path script = tempFolder.newFile("init.sql").toPath();
    Files.write(
        script,
        ("-- setup comment\n"
                + "/* block comment ; with semicolon */\n"
                + "CREATE TABLE items (id INT, label VARCHAR);\n"
                + "INSERT INTO items VALUES (1, 'alpha;one'), (2, 'beta''two');\n")
            .getBytes(UTF_8));

    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(dbFile)) {
      handle.executeScript(script);

      handle.executeQuery("SELECT 1");
      assertThrows(
          SQLException.class, () -> handle.executeQuery("SELECT * FROM nonexistent_table"));
      assertEquals("alpha;one", handle.queryForString("SELECT label FROM items WHERE id = 1"));
      assertNull(handle.queryForString("SELECT NULL"));
      assertThrows(
          SQLException.class,
          () -> handle.queryForString("SELECT label FROM items WHERE id = 999"));
      assertThrows(SQLException.class, () -> handle.queryForString("SELECT label FROM items"));
      assertThrows(
          SQLException.class,
          () -> handle.queryForString("SELECT id, label FROM items WHERE id = 1"));
      assertEquals(
          ImmutableList.of("alpha;one", "beta'two"),
          handle.queryStringColumn("SELECT label FROM items ORDER BY id", "label"));
    }
  }

  @Test
  public void jdbcHandle_scriptStatementFails_messageNamesStatement() throws Exception {
    File dbFile = DumperTestUtils.newJdbcFile("handle_util_failing_script_test");
    dbFile.delete();
    Path script = tempFolder.newFile("failing.sql").toPath();
    Files.write(script, "SELECT 1;\nSELECT * FROM nonexistent_table;\nSELECT 3;".getBytes(UTF_8));

    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(dbFile)) {
      SQLException exception = assertThrows(SQLException.class, () -> handle.executeScript(script));

      String expectedStart =
          "Failed to execute SQL script statement #2 of file ["
              + script.toAbsolutePath()
              + "]: SELECT * FROM nonexistent_table; nested exception is ";
      assertEquals(expectedStart, exception.getMessage().substring(0, expectedStart.length()));
      assertNotNull(exception.getCause());
    }
  }

  @Test
  public void splitSqlStatements_handlesCommentsAndQuotes() {
    String script =
        "-- line comment;\n"
            + "CREATE TABLE t (a TEXT, \"b;col\" INT);\n"
            + "/* block; comment */\n"
            + "INSERT INTO t VALUES ('semi;colon', 1);\n"
            + "INSERT INTO t VALUES ('escaped''quote', 2)";

    assertEquals(
        ImmutableList.of(
            "CREATE TABLE t (a TEXT, \"b;col\" INT)",
            "INSERT INTO t VALUES ('semi;colon', 1)",
            "INSERT INTO t VALUES ('escaped''quote', 2)"),
        JdbcHandle.splitSqlStatements(script));
  }

  @Test
  public void splitSqlStatements_noSemicolon_splitsAtLineEnds() {
    assertEquals(
        ImmutableList.of("SELECT 1", "SELECT 2", "SELECT 3"),
        JdbcHandle.splitSqlStatements("SELECT 1\nSELECT 2\r\n\nSELECT 3\n"));
  }

  @Test
  public void splitSqlStatements_semicolon_lineEndsDoNotSplit() {
    assertEquals(
        ImmutableList.of("SELECT a, b FROM t", "SELECT 2"),
        JdbcHandle.splitSqlStatements("SELECT a,\n  b\nFROM t;\nSELECT 2"));
  }

  @Test
  public void splitSqlStatements_whitespace_collapsedOutsideQuotesOnly() {
    assertEquals(
        ImmutableList.of("SELECT 'a   b' , 2 "),
        JdbcHandle.splitSqlStatements("  SELECT   'a   b'\t,\n2 ;"));
  }

  @Test
  public void splitSqlStatements_backslash_keepsNextCharacter() {
    assertEquals(
        ImmutableList.of("SELECT 'a\\'b;c'", "SELECT 2"),
        JdbcHandle.splitSqlStatements("SELECT 'a\\'b;c'; SELECT 2"));
  }

  @Test
  public void splitSqlStatements_commentAtEndOfLine_removedWithLineEnd() {
    assertEquals(
        ImmutableList.of("SELECT 1 FROM t"),
        JdbcHandle.splitSqlStatements("SELECT 1 -- comment\nFROM t;"));
  }

  @Test
  public void splitSqlStatements_onlyCommentLines_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JdbcHandle.splitSqlStatements("-- comment\n \n-- comment\n"));
  }

  @Test
  public void splitSqlStatements_blockCommentNotClosed_throws() {
    assertThrows(
        IllegalArgumentException.class,
        () -> JdbcHandle.splitSqlStatements("SELECT 1; /* not closed"));
  }

  @Test
  public void splitSqlStatements_blockCommentStartsWithSlash_removedUpToItsEnd() {
    assertEquals(
        ImmutableList.of("SELECT 1", "SELECT 2"),
        JdbcHandle.splitSqlStatements("SELECT 1; /*/ comment */ SELECT 2;"));
  }

  @Test
  public void splitSqlStatements_slashAfterBlockCommentStart_doesNotCloseComment() {
    assertThrows(
        IllegalArgumentException.class, () -> JdbcHandle.splitSqlStatements("SELECT 1; /*/"));
  }

  @Test
  public void splitSqlStatements_emptyBlockComment_removed() {
    assertEquals(
        ImmutableList.of("SELECT 1", "SELECT 2"),
        JdbcHandle.splitSqlStatements("SELECT 1; /**/ SELECT 2;"));
  }
}
