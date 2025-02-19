package com.google.edwmigration.dbsync.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

public class StorageTestRunner {

  private final ByteSource byteSource;
  private final ByteSink byteSink;

  public StorageTestRunner(ByteSource byteSource, ByteSink byteSink) {
    this.byteSource = byteSource;
    this.byteSink = byteSink;
  }

  public void run() throws IOException {
    ByteSource data = RsyncTestRunner.newRandomData(20 * 1024 * 1024);

    WRITE:
    {
      data.copyTo(byteSink);
    }

    READ:
    {
      ByteArrayOutputStream sink = new ByteArrayOutputStream();
      byteSource.copyTo(sink);
      assertArrayEquals(data.read(), byteSource.read(), "Read got mismatched data.");
      assertArrayEquals(data.read(), sink.toByteArray(), "Read/write got mismatched data.");
    }

    SLICE:
    {
      ByteArrayOutputStream sink = new ByteArrayOutputStream();
      ByteSource slice = data.slice(1024, 1024);
      ByteSource source = byteSource.slice(1024, 1024);
      source.copyTo(sink);
      assertArrayEquals(slice.read(), source.read(), "Slice got mismatched data.");
      assertArrayEquals(slice.read(), sink.toByteArray(), "Slice/write got mismatched data.");
    }

  }
}
