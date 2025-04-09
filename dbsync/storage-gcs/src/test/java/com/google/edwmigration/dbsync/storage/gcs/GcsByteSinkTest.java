package com.google.edwmigration.dbsync.storage.gcs;

import com.google.edwmigration.dbsync.test.StorageTestRunner;
import java.net.URI;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GcsByteSinkTest {

  @Disabled
  @Test
  public void testGcsByteSink() throws Exception {
    GcsStorage storage = new GcsStorage("");
    URI uri = URI.create("");
    StorageTestRunner runner =
        new StorageTestRunner(storage.newByteSource(uri), storage.newByteSink(uri));
    runner.run();
  }
}
