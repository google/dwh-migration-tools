package com.google.edwmigration.dbsync.common;

import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.edwmigration.dbsync.test.RsyncTestRunner;
import com.google.common.io.ByteSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstructionGeneratorTest {
  private static final Logger logger = LoggerFactory.getLogger(InstructionGeneratorTest.class);

  @Test
  public void testChecksumMatcher() throws Exception {
    // Do not make these be "sensible" constants. That's not the point.
    ByteSource srcData = RsyncTestRunner.newRandomData(65531);

    ChecksumGenerator generator = new ChecksumGenerator(1025);
    List<Checksum> checksums = new ArrayList<>();
    generator.generate(checksums::add, srcData);
    // logger.info("Checksums are " + checksums);

    byte[] dstData = new byte[17612];
    ThreadLocalRandom.current().nextBytes(dstData);
    System.arraycopy(srcData.read(), 14, dstData, 234, 17000);
    dstData[12876]--;

    InstructionGenerator matcher = new InstructionGenerator(generator.getBlockSize());
    matcher.generate(i -> logger.info(String.valueOf(i)), ByteSource.wrap(dstData), checksums);
  }

}