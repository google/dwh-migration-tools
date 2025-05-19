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
package com.google.edwmigration.permissions.commands.expand;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class CsvFileStreamProcessorTest {

  @AutoValue
  @JsonSerialize(as = TestObject.class)
  public abstract static class TestObject {

    @JsonCreator
    public static TestObject create(
        @JsonProperty("string_field") String stringField, @JsonProperty("int_value") int intField) {
      return new AutoValue_CsvFileStreamProcessorTest_TestObject(stringField, intField);
    }

    @JsonProperty("string_field")
    public abstract String stringField();

    @JsonProperty("int_field")
    public abstract int intField();
  }

  public static final CsvMapper CSV_MAPPER = new CsvMapper();

  @Test
  public void process_parsesPlainCsvFile() throws URISyntaxException {
    Path csvFile = Paths.get(Resources.getResource("csv/test.csv").toURI());
    CsvFileStreamProcessor<TestObject> csvFileStreamProcessor =
        new CsvFileStreamProcessor<>(
            CSV_MAPPER,
            csvFile.getParent().toString(),
            csvFile.getFileName().toString(),
            TestObject.class);

    List<TestObject> actual =
        csvFileStreamProcessor.process(Function.identity()).collect(toImmutableList());

    ImmutableList<TestObject> expected =
        ImmutableList.of(
            TestObject.create("hello world", 42), TestObject.create("hello universe", 43));
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void process_parsesCsvFileFromZipArchive() throws URISyntaxException {
    Path zipFile = Paths.get(Resources.getResource("csv/test.zip").toURI());
    CsvFileStreamProcessor<TestObject> csvFileStreamProcessor =
        new CsvFileStreamProcessor<>(CSV_MAPPER, zipFile.toString(), "test.csv", TestObject.class);

    List<TestObject> actual =
        csvFileStreamProcessor.process(Function.identity()).collect(toImmutableList());

    ImmutableList<TestObject> expected =
        ImmutableList.of(
            TestObject.create("hello world", 42), TestObject.create("hello universe", 43));
    assertThat(actual).containsExactlyElementsIn(expected);
  }
}
