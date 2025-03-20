package com.google.edwmigration.dbsync.common.storage;

import com.google.edwmigration.dbsync.test.StorageTestRunner;
import java.io.File;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalStorageTest {

  private static final Logger logger = LoggerFactory.getLogger(LocalStorageTest.class);

  @Test
  public void testLocalStorage(@TempDir File tempDir) throws Exception {
    LocalStorage storage = new LocalStorage();
    File file = new File(tempDir, "test.dat");
    logger.info("File is {}", file);
    StorageTestRunner runner = new StorageTestRunner(
        storage.newByteSource(file),
        storage.newByteSink(file));
    runner.run();
  }

}