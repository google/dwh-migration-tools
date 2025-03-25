package com.google.edwmigration.dbsync.common;

import static org.junit.jupiter.api.Assertions.*;

import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.test.RsyncTestRunner;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.common.math.IntMath;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChecksumGeneratorTest {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(ChecksumGeneratorTest.class);

  // Do not make these be "sensible" constants. That's not the point.
  private static final int DATA_SIZE = 65531;
  private static final int BLOCK_SIZE = 1025;

  @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
  public void testChecksumGenerator() throws Exception {
    int dataSize = DATA_SIZE + ThreadLocalRandom.current().nextInt(DATA_SIZE);
    ByteSource data = RsyncTestRunner.newRandomData(dataSize);

    int blockSize = BLOCK_SIZE + ThreadLocalRandom.current().nextInt(42);
    logger.info("Checksumming {} bytes in blocks of {} bytes.", dataSize, blockSize);

    ChecksumGenerator generator = new ChecksumGenerator(blockSize);
    List<Checksum> checksums = new ArrayList<>();
    generator.generate(checksums::add, data);
    logger.info("Checksums are " + checksums);

    int blockCount = IntMath.divide(dataSize, blockSize, RoundingMode.CEILING);
    assertEquals(blockCount, checksums.size());
    for (int i = 0; i < blockCount; i++) {
      Checksum c = checksums.get(i);
      assertEquals(i * blockSize, c.getBlockOffset(),"Bad offset in " + c);
      ByteSource block = data.slice(c.getBlockOffset(), c.getBlockLength());
      HashCode strongHashCode = block.hash(RollingChecksumImpl.STRONG_HASH_FUNCTION);
      assertEquals(strongHashCode.toString(), c.getStrongChecksum(), "Bad hash code in " + c);
    }
  }
}