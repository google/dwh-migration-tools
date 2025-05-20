package com.google.edwmigration.dbsync.common;

import com.google.common.io.ByteSink;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ByteArraySink extends ByteSink {

  private static final byte[] EMPTY = new byte[0];
  private byte[] data = EMPTY;

  public byte[] getData() {
    return data;
  }

  @Override
  public OutputStream openStream() throws IOException {
    return new ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        ByteArraySink.this.data = toByteArray();
      }
    };
  }
}
