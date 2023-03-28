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
package com.google.edwmigration.dumper.application.dumper.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContext;
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
public class JdbcSelectTaskTest extends AbstractTaskTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(JdbcSelectTaskTest.class);

  private static final String NAME = JdbcSelectTaskTest.class.getSimpleName();
  private static final File FILE = DumperTestUtils.newJdbcFile(NAME);
  private static final String QUERY = "SELECT a, b, c FROM foo";

  private enum Header {
    Foo,
    Bar,
    Baz
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName("org.sqlite.JDBC");
    LOG.info("Writing to sqlite database: {}", FILE.getAbsolutePath());
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
      new JdbcSelectTask("(memory)", QUERY).doRun(new DummyTaskRunContext(handle), sink, handle);
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
          .doRun(new DummyTaskRunContext(handle), sink, handle);
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
      task.doRun(new DummyTaskRunContext(handle), sink, handle);
    }
    String actualOutput = sink.openStream().toString();
    assertEquals("Foo,Bar,Baz\n,14,3\n", actualOutput);

    CSVFormat format = formatHolder.getValue();
    assertNotNull("CSVFormat was null.", format);
    LOG.info("Format is " + format);
    LOG.info("Format.nullString is " + format.getNullString());

    // File file = TestUtils.newOutputFile("dumper-format-test.csv");
  }
}
