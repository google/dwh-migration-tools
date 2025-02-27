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
package com.google.edwmigration.dumper.application.dumper.connector.ranger;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class RangerTestResources {

  private RangerTestResources() {}

  public static String getResourceAsString(String name) throws IOException {
    return Resources.toString(Resources.getResource(name), UTF_8);
  }

  public static InputStream getResourceAsInputStream(String name) throws IOException {
    return new ByteArrayInputStream(Resources.toByteArray(Resources.getResource(name)));
  }

  public static String getResourceAbsolutePath(String name) throws URISyntaxException {
    return new File(Resources.getResource(name).toURI()).getAbsolutePath();
  }
}
