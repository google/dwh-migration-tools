package com.google.edwmigration.dbsync.storage.gcs;

import com.google.edwmigration.dbsync.test.StorageTestRunner;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class GcsByteSinkTest {

  @Test
  public void testGcsByteSink() throws Exception {
    GcsStorage storage = new GcsStorage("bigquerytestdefault");
    URI uri = URI.create("gs://shevek-test/test-file");
    StorageTestRunner runner = new StorageTestRunner(
        storage.newByteSource(uri),
        storage.newByteSink(uri));
    runner.run();
  }
}