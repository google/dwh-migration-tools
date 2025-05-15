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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectReader;
import com.google.edwmigration.permissions.files.FileProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.function.Function;
import java.util.stream.Stream;

public class JsonlFileStreamProcessor<T> implements StreamProcessor<T> {

  private final ObjectReader objectReader;

  private final String path;

  private final String file;

  private final Class<T> recordClass;

  public JsonlFileStreamProcessor(
      ObjectReader objectReader, String path, String file, Class<T> recordClass) {
    this.objectReader = objectReader;
    this.path = path;
    this.file = file;
    this.recordClass = recordClass;
  }

  @Override
  public <R> R process(Function<Stream<T>, R> operator) {
    return FileProcessor.apply(
        path,
        directory -> {
          try (Reader reader =
                  Channels.newReader(Files.newByteChannel(directory.resolve(file)), UTF_8.name());
              BufferedReader bufferedReader = new BufferedReader(reader)) {
            Stream<T> stream =
                bufferedReader
                    .lines()
                    .map(
                        json -> {
                          try {
                            return objectReader.readValue(json, recordClass);
                          } catch (IOException e) {
                            throw new IllegalArgumentException(e);
                          }
                        });
            return operator.apply(stream);
          }
        });
  }
}
