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
package com.google.edwmigration.dumper.application.dumper.connector.hadoop.oozie;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractOozieJobsTaskTest {

  static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  @Test
  public void toRecordProperty_dateArgument_convertsToLong() throws Exception {
    Date christmas = dateFormat.parse("2025-12-25");

    Object result = AbstractOozieJobsTask.toRecordProperty(christmas);

    assertEquals(Long.class, result.getClass());
    assertEquals("2025-12-25", convertDate(result));
  }

  @Test
  public void toRecordProperty_listArgument_success() throws Exception {
    ImmutableList<String> list = ImmutableList.of("abcde", "fghi");

    Object result = AbstractOozieJobsTask.toRecordProperty(list);

    assertEquals("[\"abcde\",\"fghi\"]", result);
  }

  @Test
  public void toRecordProperty_otherArgument_nothingChanges() throws Exception {
    Object value = new Object();

    Object result = AbstractOozieJobsTask.toRecordProperty(value);

    assertEquals(value, result);
  }

  private static String convertDate(Object date) {
    if (date instanceof Long) {
      return dateFormat.format(new Date((Long) date));
    }
    throw new RuntimeException();
  }
}
