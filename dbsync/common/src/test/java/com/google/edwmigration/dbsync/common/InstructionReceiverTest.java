package com.google.edwmigration.dbsync.common;

import com.google.edwmigration.dbsync.test.RsyncTestRunner;
import com.google.common.io.ByteSource;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstructionReceiverTest {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(InstructionReceiverTest.class);

  private void testInstructionReceiver(String name, byte[] serverData, byte[] clientData, int blockSize) throws Exception {
    RsyncTestRunner runner = new RsyncTestRunner(name,
        ByteSource.wrap(serverData),
        ByteSource.wrap(clientData));
    runner.setFlags(RsyncTestRunner.Flag.values());
    runner.setBlockSize(blockSize);
    runner.run();
  }

  @Test
  public void testInstructionReceiverIrregular() throws Exception {
    byte[] serverData = new byte[0];
    byte[] clientData = new byte[8];
    ThreadLocalRandom.current().nextBytes(clientData);
    testInstructionReceiver("Irregular", serverData, clientData, 3);
  }

  @Test
  public void testInstructionReceiverSimple() throws Exception {
    byte[] serverData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    byte[] clientData = new byte[] { 0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 42, 43, 44, 13, 14, 15, 16 };
    testInstructionReceiver("Simple", serverData, clientData, 4);
  }

}
