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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.ByteSink;
import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

public class JsonWriter implements Closeable {

  private final Writer writer;

  public JsonWriter(@Nonnull ByteSink sink) throws IOException {
    this.writer = openStream(sink);
  }

  Writer openStream(ByteSink sink) throws IOException {
    return sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
  }

  public void writeLine(JsonNode json) throws IOException {
    writer.write(json.toString());
    writer.write('\n');
  }

  public void writeLine(String json) throws IOException {
    writer.write(json);
    writer.write('\n');
  }

  public void write(String json) throws IOException {
    writer.write(json);
  }

  @Override
  public void close() throws IOException {
    this.writer.close();
  }
}
