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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import java.util.List;
import org.apache.hadoop.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnowflakeTaskUtilTest {

  enum Header {
    A,
    B,
    C
  }

  @Test
  public void withFilter_whereConditions() {

    assertEquals(
        "SELECT A, B, C FROM SCHEMA.TABLE", getSqlForWhereConditions(Lists.newArrayList()));

    assertEquals(
        "SELECT A, B, C FROM SCHEMA.TABLE",
        getSqlForWhereConditions(Lists.newArrayList((String) null)));

    assertEquals(
        "SELECT A, B, C FROM SCHEMA.TABLE WHERE A",
        getSqlForWhereConditions(Lists.newArrayList("A")));

    assertEquals(
        "SELECT A, B, C FROM SCHEMA.TABLE WHERE A AND B",
        getSqlForWhereConditions(Lists.newArrayList("A", null, "B")));
  }

  private static String getSqlForWhereConditions(List<String> whereConditions) {
    AbstractJdbcTask<Summary> task =
        SnowflakeTaskUtil.withFilter(
            /* format= */ "SELECT A, B, C FROM %1$s.TABLE%2$s",
            /* schemaName= */ "SCHEMA",
            /* zipEntryName= */ "file.csv",
            whereConditions,
            Header.class);
    JdbcSelectTask selectTask = (JdbcSelectTask) task;
    return selectTask.getSql();
  }
}
