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

import static com.google.common.base.Verify.verifyNotNull;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.edwmigration.permissions.files.FileProcessor;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class CsvFileStreamProcessor<T> implements StreamProcessor<T> {

  private final CsvMapper csvMapper;

  private final String path;

  private final String file;

  private final Class<T> recordClass;

  public CsvFileStreamProcessor(
      CsvMapper csvMapper, String path, String file, Class<T> recordClass) {
    this.csvMapper = csvMapper;
    this.path = verifyNotNull(path);
    this.file = file;
    this.recordClass = recordClass;
  }

  @Override
  public <R> R process(Function<Stream<T>, R> operator) {
    return FileProcessor.apply(
        path,
        directory -> {
          List<T> entries;
          CsvSchema schema =
              csvMapper.typedSchemaFor(this.recordClass).withHeader().withColumnReordering(true);
          try (InputStream is = Files.newInputStream(directory.resolve(file));
              MappingIterator<T> iterator =
                  csvMapper.readerFor(this.recordClass).with(schema).readValues(is)) {
            entries = iterator.readAll();
          }
          return operator.apply(entries.stream());
        });
  }
}
