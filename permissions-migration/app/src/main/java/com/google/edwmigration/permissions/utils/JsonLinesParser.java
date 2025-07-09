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
package com.google.edwmigration.permissions.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class JsonLinesParser {

  private final ObjectMapper objectMapper;

  public JsonLinesParser(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public <T> Stream<T> parseFile(Path file, Class<T> lineClass) throws IOException {
    return Files.lines(file)
        .map(
            json -> {
              try {
                return objectMapper.readValue(json, lineClass);
              } catch (IOException e) {
                throw new IllegalArgumentException(e);
              }
            });
  }
}
