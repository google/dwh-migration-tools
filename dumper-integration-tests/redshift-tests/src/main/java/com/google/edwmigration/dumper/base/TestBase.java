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
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with general values for all TestNG test suites */
public abstract class TestBase {

  public static final CSVParser CSV_PARSER = new CSVParserBuilder().withEscapeChar('\0').build();
  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  /**
   * @param dbMultiset List of extracted from DB items
   * @param csvMultiset List of uploaded from Avro items
   */
  public static void assertMultisetsEqual(
      LinkedHashMultiset<?> dbMultiset, LinkedHashMultiset<?> csvMultiset) {
    LinkedHashMultiset<?> dbMultisetCopy = LinkedHashMultiset.create(dbMultiset);
    csvMultiset.forEach(dbMultiset::remove);
    dbMultisetCopy.forEach(csvMultiset::remove);

    String dbListForLogs = lineSeparator() + Joiner.on("").join(dbMultiset);
    String csvListForLogs = lineSeparator() + Joiner.on("").join(csvMultiset);

    if (dbMultiset.isEmpty() && csvMultiset.isEmpty()) {
      LOGGER.info("DB view and CSV file are equal");
    } else if (!dbMultiset.isEmpty() && !csvMultiset.isEmpty()) {
      Assert.fail(
          format(
              "DB view and CSV file have mutually exclusive row(s)%n"
                  + "DB view has %d different row(s): %s%n"
                  + "CSV file has %d different row(s): %s",
              dbMultiset.size(), dbListForLogs, csvMultiset.size(), csvListForLogs));
    } else if (!dbMultiset.isEmpty()) {
      Assert.fail(format("DB view has %d extra row(s):%n%s", dbMultiset.size(), dbListForLogs));
    } else if (!csvMultiset.isEmpty()) {
      Assert.fail(format("CSV file has %d extra row(s):%n%s", csvMultiset.size(), csvListForLogs));
    }
  }
}
