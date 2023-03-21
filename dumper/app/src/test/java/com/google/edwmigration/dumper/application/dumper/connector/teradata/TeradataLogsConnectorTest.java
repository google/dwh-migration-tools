/*
 * Copyright 2022-2023 Google LLC
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

import static com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask.setParameterValues;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.ResourceLocation;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.io.FileSystemOutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandleFactory;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContext;
import com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.UrlResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.util.FileSystemUtils;

/** @author shevek */
@RunWith(JUnit4.class)
public class TeradataLogsConnectorTest extends AbstractConnectorExecutionTest {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnectorTest.class);

  private static final int N_QUERY_LOGS = 2_000_000;

  private static boolean exec(@Nonnull PreparedStatement statement, Object... arguments)
      throws SQLException {
    setParameterValues(statement, arguments);
    return statement.execute();
  }

  @Nonnull
  private static byte[] newUserName(int i_log) {
    byte i_user0 = (byte) (i_log >> 24);
    byte i_user1 = (byte) (i_log >> 16);
    byte i_user2 = (byte) (i_log >> 8);
    byte i_user3 = (byte) i_log;
    return new byte[] {i_user3, i_user2, i_user1, i_user0};
  }

  private final TeradataLogsConnector connector = new TeradataLogsConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void testExecution() throws Exception {
    String name = getClass().getSimpleName();
    File dbFile = DumperTestUtils.newJdbcFile(name);
    File zipFile = DumperTestUtils.newZipFile(name);

    dbFile.delete();
    zipFile.delete();

    URI outputUri = URI.create("jar:" + zipFile.toURI());

    // This isn't great because all the column-validity queries fail.
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(dbFile);
        FileSystem fileSystem =
            FileSystems.newFileSystem(outputUri, ImmutableMap.of("create", "true"))) {
      OutputHandleFactory sinkFactory = new FileSystemOutputHandleFactory(fileSystem, "/");
      handle.getJdbcTemplate().execute("attach ':memory:' as dbc");
      // handle.getJdbcTemplate().execute("create table dbc.dbcinfo (InfoKey varchar,  InfoData
      // varchar)");
      handle
          .getJdbcTemplate()
          .execute(
              "create table "
                  + TeradataLogsConnector.DEF_LOG_TABLE
                  + " (UserName varchar, errorcode int, StartTime int)");

      TaskRunContext runContext = new DummyTaskRunContext(sinkFactory, handle);
      List<Task<?>> tasks = new ArrayList<>();
      connector.addTasksTo(
          tasks,
          new ConnectorArguments(
              new String[] {"--connector", connector.getName(), "--query-log-days", "1"}));
      for (Task<?> task : tasks) {
        task.run(runContext);
      }
    }
  }

  @Test
  public void testConnectorSynthetic() throws Exception {
    Assume.assumeTrue(isDumperTest());

    if (false) {
      DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql:cw", "cw", "password");
      LOG.debug("Resetting database...");
      try (Connection connection = dataSource.getConnection()) {
        URL url =
            Resources.getResource(
                ResourceLocation.class, "teradata-simulation-schema-query-logs.sql");
        ScriptUtils.executeSqlScript(connection, new UrlResource(url));
      }
      JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

      LOG.debug("Rebuilding database....");
      try (ProgressMonitor monitor =
          new RecordProgressMonitor("Creating sample schema", N_QUERY_LOGS).withBlockSize(256)) {
        jdbcTemplate.execute(
            new ConnectionCallback<Void>() {
              @Override
              public Void doInConnection(Connection connection)
                  throws SQLException, DataAccessException {
                PreparedStatement dbqlogtbl =
                    connection.prepareStatement(
                        "insert into DBC.DBQLogTbl (\"procid\", \"collecttimestamp\", \"username\", \"errorcode\", \"querytext\", \"queryid\") values (?, ?, ?, ?, ?, ?)");
                PreparedStatement dbqlsqltbl =
                    connection.prepareStatement(
                        "insert into DBC.DBQLSqlTbl (\"procid\", \"collecttimestamp\", \"queryid\", \"sqlrowno\", \"sqltextinfo\") values (?, ?, ?, ?, ?)");

                connection.setAutoCommit(false);
                long startTime = System.currentTimeMillis();
                final int errorCode = 0; // entries with non-zero codes won't get dumped
                for (int i_log = 0; i_log < N_QUERY_LOGS; i_log++) {
                  int procId = i_log % 10;
                  Timestamp collectTimeStamp =
                      new Timestamp(startTime - i_log); // decrement by one millisecond
                  final String fullQueryText =
                      "select " + Strings.repeat("'" + i_log + "', ", 50) + "'end';";
                  final String abbrevQueryText =
                      fullQueryText.substring(
                          0, 100); // not actual TD behavior, but conveys the point
                  exec(
                      dbqlogtbl,
                      procId,
                      collectTimeStamp,
                      newUserName(i_log),
                      errorCode,
                      abbrevQueryText,
                      i_log);
                  exec(dbqlsqltbl, procId, collectTimeStamp, i_log, 0, fullQueryText);
                  monitor.count();
                }
                connection.commit();

                return null;
              }
            });
      }
    }

    File outputFile = TestUtils.newOutputFile("compilerworks-teradata-logs-synthetic.zip");
    FileSystemUtils.deleteRecursively(outputFile);

    List<String> args =
        Arrays.asList(
            "--connector", connector.getName(),
            "--output", outputFile.getAbsolutePath(),
            "--jdbcDriver", org.postgresql.Driver.class.getName(),
            "--url", "jdbc:postgresql:cw",
            "--user", "cw",
            "--password", "password",
            "--query-log-days", "1");

    // Rewrite validity queries from Teradata (SELECT TOP 1 ...) to PostgreSQL:
    TeradataLogsJdbcTask.EXPRESSION_VALIDITY_QUERY = "SELECT %s FROM %s FETCH FIRST 1 ROW ONLY";

    MetadataDumper dumper = new MetadataDumper();
    dumper.run(args.toArray(ArrayUtils.EMPTY_STRING_ARRAY));

    // TODO: Use ZipValidator to assert that all N_QUERY_LOGS entries are present.
  }

  // exprs are usuall names, .. if not this test is junk
  private void checkNames(Enum<?>[] vals, String[] exprs) {
    assertEquals(vals.length, exprs.length);
    for (int j = 0; j < vals.length; j++) Assert.assertTrue(exprs[j].endsWith(vals[j].name()));
  }

  @Test
  public void testHeaderAndColumns() {
    checkNames(TeradataLogsDumpFormat.Header.values(), TeradataLogsJdbcTask.EXPRESSIONS);
    checkNames(
        TeradataLogsDumpFormat.HeaderLSql.values(),
        Teradata14LogsConnector.EXPRESSIONS_LSQL_TBL.toArray(
            new String[Teradata14LogsConnector.EXPRESSIONS_LSQL_TBL.size()]));
    checkNames(
        TeradataLogsDumpFormat.HeaderLog.values(),
        Teradata14LogsConnector.EXPRESSIONS_LOG_TBL.toArray(
            new String[Teradata14LogsConnector.EXPRESSIONS_LOG_TBL.size()]));
  }
}
