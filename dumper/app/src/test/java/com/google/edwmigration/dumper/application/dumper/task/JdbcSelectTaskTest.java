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
package com.google.edwmigration.dumper.application.dumper.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author matt */
@RunWith(JUnit4.class)
public class JdbcSelectTaskTest {

  private static final Logger logger = LoggerFactory.getLogger(JdbcSelectTaskTest.class);

  private static final String NAME = JdbcSelectTaskTest.class.getSimpleName();
  private static final File FILE = DumperTestUtils.newJdbcFile(NAME);
  private static final String QUERY = "SELECT a, b, c FROM foo";

  private enum Header {
    Foo,
    Bar,
    Baz
  }

  final TaskRunContext mockContext = mock(TaskRunContext.class);

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName("org.sqlite.JDBC");
    logger.info("Writing to sqlite database: {}", FILE.getAbsolutePath());
    FILE.delete();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      handle.getJdbcTemplate().execute("CREATE TABLE foo ( a INT, b INT, c INT )");
      handle.getJdbcTemplate().execute("INSERT INTO foo VALUES ( 1, 2, 3 )");
    }
  }

  @Test
  public void testResultSetHeader() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      new JdbcSelectTask("(memory)", QUERY).doRun(mockContext, sink, handle);
    }
    String actualOutput = sink.openStream().toString();
    assertEquals("a,b,c\n1,2,3\n", actualOutput);
  }

  @Test
  public void testClassHeader() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      new JdbcSelectTask("(memory)", QUERY)
          .withHeaderClass(Header.class)
          .doRun(mockContext, sink, handle);
    }
    String actualOutput = sink.openStream().toString();
    assertEquals("Foo,Bar,Baz\n1,2,3\n", actualOutput);
  }

  @Test
  public void testCsvFormat() throws Exception {
    String sql = "select null, 14, c FROM foo";
    MemoryByteSink sink = new MemoryByteSink();
    final MutableObject<CSVFormat> formatHolder = new MutableObject<>();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      AbstractJdbcTask<Summary> task =
          new JdbcSelectTask("(memory)", sql) {
            @Override
            protected CSVFormat newCsvFormat(ResultSet rs) throws SQLException {
              CSVFormat format = super.newCsvFormat(rs);
              formatHolder.setValue(format);
              return format;
            }
          }.withHeaderClass(Header.class);
      task.doRun(mockContext, sink, handle);
    }
    String actualOutput = sink.openStream().toString();
    assertEquals("Foo,Bar,Baz\n,14,3\n", actualOutput);

    CSVFormat format = formatHolder.getValue();
    assertNotNull("CSVFormat was null.", format);
    logger.info("Format is " + format);
    logger.info("Format.nullString is " + format.getNullString());

    // File file = TestUtils.newOutputFile("dumper-format-test.csv");
  }

  @Test
  public void toString_success() {
    JdbcSelectTask task = new JdbcSelectTask("/dir1/dir2/sample.txt", "SELECT 123;");

    String taskDescription = task.toString();

    assertEquals("Write /dir1/dir2/sample.txt from\n        SELECT 123;", taskDescription);
  }

  @Test
  public void append_success() throws Exception {
    String firstSql = "select null, 14, c FROM foo";
    String secondSql = "select null, 15, b FROM foo";

    MemoryByteSink sink = new MemoryByteSink();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      AbstractTask<Summary> first =
          new JdbcSelectTask("(memory)", firstSql).withHeaderClass(Header.class);
      AbstractTask<Summary> second =
          new JdbcSelectTask(
                  "(memory)",
                  secondSql,
                  TaskCategory.REQUIRED,
                  TaskOptions.DEFAULT.withWriteMode(WriteMode.APPEND_EXISTING))
              .withHeaderClass(Header.class);
      first.doRun(mockContext, sink, handle);
      second.doRun(mockContext, sink, handle);
    }
    String actualOutput = sink.openStream().toString();
    assertEquals("Foo,Bar,Baz\n,14,3\n,15,2\n", actualOutput);
  }
}
