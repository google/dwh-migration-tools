package com.google.edwmigration.dbsync.storage.aws;

import com.google.edwmigration.dbsync.test.StorageTestRunner;
import java.net.URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

public class S3ByteSinkTest {

  @Test
  public void testS3ByteSink() throws Exception {
    S3Storage storage = new S3Storage(Region.US_EAST_1);
    URI uri = URI.create("s3://shevek-test/test-file");
    StorageTestRunner runner = new StorageTestRunner(
        storage.newByteSource(uri),
        storage.newByteSink(uri));
    runner.run();
  }
}
