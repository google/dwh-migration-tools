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
