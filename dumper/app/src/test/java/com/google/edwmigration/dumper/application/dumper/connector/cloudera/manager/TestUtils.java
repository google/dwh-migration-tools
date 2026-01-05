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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class TestUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static String toJsonl(String json) throws Exception {
    return MAPPER.readTree(json).toString();
  }

  public static String readFileAsString(String fileName) throws IOException, URISyntaxException {
    URI uri = Objects.requireNonNull(TestUtils.class.getResource(fileName)).toURI();
    return new String(Files.readAllBytes(Paths.get(uri)));
  }
}
