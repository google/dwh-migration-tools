/*
 * Copyright 2022 Google LLC
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
package com.google.edwmigration.dumper.base;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedHashMultiset;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with general values for all TestNG test suites */
public abstract class TestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  /**
   * @param dbList List of extracted from DB items
   * @param csvList List of uploaded from Avro items
   */
  public static void assertListsEqual(
      final LinkedHashMultiset dbList, final LinkedHashMultiset csvList) {
    String dbListForLogs = lineSeparator() + Joiner.on("").join(dbList);
    String csvListForLogs = lineSeparator() + Joiner.on("").join(csvList);

    if (dbList.isEmpty() && csvList.isEmpty()) {
      LOGGER.info("DB view and CSV file are equal");
    } else if (!dbList.isEmpty() && !csvList.isEmpty()) {
      Assert.fail(
          format(
              "DB view and CSV file have mutually exclusive row(s)%n"
                  + "DB view has %d different row(s): %s%n"
                  + "CSV file has %d different row(s): %s",
              dbList.size(), dbListForLogs, csvList.size(), csvListForLogs));
    } else if (!dbList.isEmpty()) {
      Assert.fail(format("DB view has %d extra row(s):%n%s", dbList.size(), dbListForLogs));
    } else if (!csvList.isEmpty()) {
      Assert.fail(format("CSV file has %d extra row(s):%n%s", csvList.size(), csvListForLogs));
    }
  }
}
