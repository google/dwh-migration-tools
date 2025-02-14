package com.google.edwmigration.dbsync.common;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.io.ByteSource;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollingChecksumTest {

  private static final Logger LOG = LoggerFactory.getLogger(RollingChecksumTest.class);

  private static final int SIZE = 16;
  private static final int LEN = 4;

  private static void assertChecksumsEqual(RollingChecksumImpl c0, RollingChecksumImpl c1, String message) {
    long v0 = c0.getWeakHashCode();
    long v1 = c1.getWeakHashCode();

    assertEquals(Long.toHexString(v0), Long.toHexString(v1), message);
  }

  @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
  public void testRollingConstant(RepetitionInfo repetitionInfo) {
    byte[] data = new byte[SIZE];
    // A pseudo-random, but predictable constant.
    Arrays.fill(data, (byte) (42 + repetitionInfo.getCurrentRepetition() * 31));

    RollingChecksumImpl c0 = new RollingChecksumImpl(LEN);
    RollingChecksumImpl c1 = new RollingChecksumImpl(LEN);
    LOG.info("Init c0");
    c0.reset(data, 0);
    LOG.info("Init c1");
    c1.reset(data, 0);

    LOG.info("Rolling c1...");  // But not c0.

    // The checksum should be the same at every offset of c1.
    for (int i = LEN; i < data.length; i++) {
      c1.roll(data[i]);
      assertChecksumsEqual(c0, c1,"mismatch at offset " + i);
    }
  }

  @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
  public void testRollingInsert() {
    byte[] data = new byte[SIZE + 1];
    ThreadLocalRandom.current().nextBytes(data);

    RollingChecksumImpl c0 = new RollingChecksumImpl(LEN);
    RollingChecksumImpl c1 = new RollingChecksumImpl(LEN);
    LOG.info("Init c0");
    c0.reset(data, 1);
    LOG.info("Init c1");
    c1.reset(data, 0);

    LOG.info("Rolling c1...");
    c1.roll(data[LEN]);

    assertChecksumsEqual(c0, c1, "mismatch at offset " + LEN);
  }

  @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
  public void testRollingReset() throws Exception {
    byte[] data = new byte[SIZE];
    ThreadLocalRandom.current().nextBytes(data);

    RollingChecksumImpl c0 = new RollingChecksumImpl(SIZE);
    c0.reset(data, 0);
    RollingChecksumImpl c1 = new RollingChecksumImpl(SIZE);
    try (InputStream in = ByteSource.wrap(data).openStream()) {
      c1.reset(in);
    }

    assertChecksumsEqual(c0, c1, "mismatch on reset()");
  }
}