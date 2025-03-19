package com.google.edwmigration.dbsync.common.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceTest {

  private static final Logger logger = LoggerFactory.getLogger(SliceTest.class);

  @Test
  public void testReslice() {
    Slice slice = new Slice(100, 200);
    logger.debug("Slice is {}", slice);
    {
      Slice reslice = Slice.reslice(slice, 10, 20);
      logger.debug("Reslice is {}", reslice);
      assertEquals(110, reslice.getOffset(), "Start was wrong.");
      assertEquals(20, reslice.getLength(), "Length was wrong.");
      assertEquals(130, reslice.getEndExclusive(), "End was wrong.");
    }

    {
      Slice reslice = Slice.reslice(slice, 10, 1000);
      logger.debug("Reslice is {}", reslice);
      assertEquals(110, reslice.getOffset(), "Start was wrong.");
      assertEquals(190, reslice.getLength(), "Length was wrong.");
      assertEquals(300, reslice.getEndExclusive(), "End was wrong.");
    }
  }

}