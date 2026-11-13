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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.test.DumperTestUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractJdbcTaskTest {

  private static final File FILE =
      DumperTestUtils.newJdbcFile(AbstractJdbcTaskTest.class.getSimpleName());

  private static final String TEXT_QUERY = "SELECT id, val FROM cells ORDER BY id";

  private static final String NON_TEXT_QUERY = "SELECT -5 AS num, X'FB' AS bin";

  private static final ImmutableList<String> FORMULA_VALUES =
      ImmutableList.of("=1+2", "+1", "-- comment", "@x", "\tx", "\rx");

  private static final ImmutableList<String> PLAIN_VALUES = ImmutableList.of("plain", "'quoted");

  /** The output of {@link #TEXT_QUERY} as written before the sanitize-csv option existed. */
  private static final String UNSANITIZED_TEXT_OUTPUT =
      "id,val\n0,=1+2\n1,+1\n2,-- comment\n3,@x\n4,\"\tx\"\n5,\"\rx\"\n6,plain\n7,'quoted\n8,\n";

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName("org.sqlite.JDBC");
    FILE.delete();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      handle.getJdbcTemplate().execute("CREATE TABLE cells ( id INT, val TEXT )");
      int id = 0;
      for (String value : FORMULA_VALUES) {
        handle.getJdbcTemplate().update("INSERT INTO cells VALUES ( ?, ? )", id++, value);
      }

      for (String value : PLAIN_VALUES) {
        handle.getJdbcTemplate().update("INSERT INTO cells VALUES ( ?, ? )", id++, value);
      }

      handle.getJdbcTemplate().update("INSERT INTO cells VALUES ( ?, NULL )", id);
    }
  }

  @Test
  public void printAllResults_sanitizeCsvOff_outputUnchanged() throws Exception {
    String output = runQuery(TEXT_QUERY, newContext("--connector", "teradata"));

    assertEquals(UNSANITIZED_TEXT_OUTPUT, output);
  }

  @Test
  public void printAllResults_noArguments_outputUnchanged() throws Exception {
    String output = runQuery(TEXT_QUERY, mock(TaskRunContext.class));

    assertEquals(UNSANITIZED_TEXT_OUTPUT, output);
  }

  @Test
  public void printAllResults_sanitizeCsvOn_prefixesFormulaCells() throws Exception {
    String output = runQuery(TEXT_QUERY, newContext("--connector", "teradata", "--sanitize-csv"));

    List<String> expected = new ArrayList<>();
    for (String value : FORMULA_VALUES) {
      expected.add("'" + value);
    }

    expected.addAll(PLAIN_VALUES);
    expected.add("");
    assertEquals(expected, parseColumn(output, "val"));
  }

  @Test
  public void printAllResults_sanitizeCsvOn_numbersAndBinaryUnchanged() throws Exception {
    String output =
        runQuery(NON_TEXT_QUERY, newContext("--connector", "teradata", "--sanitize-csv"));

    assertEquals("num,bin\n-5,+w==\n", output);
  }

  @Test
  public void printAllResults_sanitizeCsvOff_numbersAndBinaryUnchanged() throws Exception {
    String output = runQuery(NON_TEXT_QUERY, newContext("--connector", "teradata"));

    assertEquals("num,bin\n-5,+w==\n", output);
  }

  private static TaskRunContext newContext(String... args) throws Exception {
    TaskRunContext context = mock(TaskRunContext.class);
    when(context.getArguments()).thenReturn(new ConnectorArguments(args));
    return context;
  }

  private static String runQuery(String sql, TaskRunContext context) throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    try (JdbcHandle handle = DumperTestUtils.newJdbcHandle(FILE)) {
      new JdbcSelectTask("(memory)", sql).doRun(context, sink, handle);
    }
    return new String(sink.toByteArray(), UTF_8);
  }

  private static List<String> parseColumn(String csv, String column) throws Exception {
    List<String> values = new ArrayList<>();
    try (CSVParser parser = CSVParser.parse(csv, AbstractTask.FORMAT.withFirstRecordAsHeader())) {
      for (CSVRecord record : parser) {
        values.add(record.get(column));
      }
    }
    return values;
  }
}
