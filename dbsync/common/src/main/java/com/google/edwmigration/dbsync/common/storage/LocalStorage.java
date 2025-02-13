package com.google.edwmigration.dbsync.common.storage;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import java.io.File;
import java.net.URI;

public class LocalStorage {
  private static File toFile(URI uri) {
    return new File(uri);
  }

  public ByteSource newByteSource(File file) {
    return Files.asByteSource(file);
  }

  public ByteSource newByteSource(URI uri) {
    return newByteSource(toFile(uri));
  }

  public ByteSink newByteSink(File file) {
    return Files.asByteSink(file);
  }

  public ByteSink newByteSink(URI uri) {
    return newByteSink(toFile(uri));
  }
}
