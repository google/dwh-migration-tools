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
import static java.util.stream.Collectors.toList;

import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
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
   * @param dbMultiset List of extracted from DB items.
   * @param csvMultiset List of uploaded from Avro items.
   *     <p>This custom method exists due to Java out of heap memory error in
   *     Truth.assertThat(dbMultiset).containsExactlyElementsIn(csvMultiset); It probably happens
   *     because .containsExactlyElementsIn() tries to print out not only the diff, let's say 1
   *     element, but entire collections.
   */
  public static void assertDbCsvDataEqual(
      LinkedHashMultiset<?> dbMultiset, LinkedHashMultiset<?> csvMultiset) {
    Multiset<?> dbReducedOnCsv = Multisets.difference(dbMultiset, csvMultiset);
    Multiset<?> csvReducedOnDb = Multisets.difference(csvMultiset, dbMultiset);

    String dbDiffEntries =
        dbReducedOnCsv.stream().map(e -> e.toString() + "\n").collect(toList()).toString();
    String csvDiffEntries =
        csvReducedOnDb.stream().map(e -> e.toString() + "\n").collect(toList()).toString();

    if (dbReducedOnCsv.isEmpty() && csvReducedOnDb.isEmpty()) {
      LOGGER.info("DB view and CSV file are equal");
    } else if (!dbReducedOnCsv.isEmpty() && !csvReducedOnDb.isEmpty()) {
      Assert.fail(
          format(
              "DB view and CSV file have mutually exclusive row(s)%n"
                  + "DB view has %d different row(s): %s%n"
                  + "CSV file has %d different row(s): %s",
              dbReducedOnCsv.size(), dbDiffEntries, csvReducedOnDb.size(), csvDiffEntries));
    } else if (!dbReducedOnCsv.isEmpty()) {
      Assert.fail(format("DB view has %d extra row(s):%n%s", dbReducedOnCsv.size(), dbDiffEntries));
    } else if (!csvReducedOnDb.isEmpty()) {
      Assert.fail(
          format("CSV file has %d extra row(s):%n%s", csvReducedOnDb.size(), csvDiffEntries));
    }
  }
}
