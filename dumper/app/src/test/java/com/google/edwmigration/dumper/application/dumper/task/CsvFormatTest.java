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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.StringReader;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@RunWith(JUnit4.class)
public class CsvFormatTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(CsvFormatTest.class);

  @Test
  public void testCsvRoundTrip() throws IOException {
    Object[] data = {"a", "b ", " c", " d "};
    StringBuilder buf = new StringBuilder();
    try (CSVPrinter printer = new CSVPrinter(buf, AbstractTask.FORMAT)) {
      printer.printRecord(data);
    }
    LOG.debug("CSV = " + buf);
    try (StringReader reader = new StringReader(buf.toString());
        CSVParser parser = new CSVParser(reader, AbstractTask.FORMAT)) {
      CSVRecord record = Iterables.getOnlyElement(parser);
      LOG.debug("Record = " + record);
      assertEquals("Bad length", data.length, record.size());
      for (int i = 0; i < data.length; i++) assertEquals("Bad field " + i, data[i], record.get(i));
    }
  }
}
