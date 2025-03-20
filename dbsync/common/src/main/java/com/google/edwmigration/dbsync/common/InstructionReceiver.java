package com.google.edwmigration.dbsync.common;

import com.google.edwmigration.dbsync.proto.BlockLocation;
import com.google.edwmigration.dbsync.proto.Instruction;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.CheckForSigned;
import javax.annotation.WillCloseWhenClosed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstructionReceiver implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(InstructionReceiver.class);

  private static final boolean DEBUG = false;

  private final OutputStream out;
  private final ByteSource in;
  @CheckForSigned
  private long copyStart = -1;
  private long copyLength = 0;

  public InstructionReceiver(@WillCloseWhenClosed OutputStream out, ByteSource in) {
    this.out = Preconditions.checkNotNull(out, "Output was null.");
    this.in = Preconditions.checkNotNull(in, "Input was null.");
  }

  public void receive(Instruction instruction) throws IOException {
    switch (instruction.getBodyCase()) {
      case BLOCKLOCATION:
        BlockLocation match = instruction.getBlockLocation();
        if (copyStart + copyLength == match.getBlockOffset()) {
          // We have a consecutive copy. This cannot happen if copyStart == -1.
          copyLength += match.getBlockLength();
        } else {
          // We have either no copy, or a non-consecutive copy.
          flushCopy();
          copyStart = match.getBlockOffset();
          copyLength = match.getBlockLength();
        }
        break;
      case DATA:
        flushCopy();
        ByteString data = instruction.getData();
        data.writeTo(out);
        break;
      default:
        throw new IllegalArgumentException("Unknown instruction type " + instruction.getClass());
    }
  }

  public void close() throws IOException {
    try {
      flushCopy();
    } finally {
      out.close();
    }
  }

  private void flushCopy() throws IOException {
    if (copyStart == -1) {
      return;
    }
    if (DEBUG) {
      logger.info(String.format("Reuse bytes from %d for %d bytes", copyStart, copyLength));
    }
    in.slice(copyStart, copyLength).copyTo(out);

    // Clear pending copy.
    copyStart = -1;
    copyLength = 0;
  }

}