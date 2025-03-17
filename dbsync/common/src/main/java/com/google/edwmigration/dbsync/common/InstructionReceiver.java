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

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(InstructionReceiver.class);

  private static final boolean DEBUG = false;

  private final OutputStream out;
  private final ByteSource in;
  private InputStream sourceStream;
  private long currentSourceOffset = 0;
  @CheckForSigned
  private long copyStart = -1;
  private long copyLength = 0;

  public InstructionReceiver(@WillCloseWhenClosed OutputStream out, ByteSource in)
      throws IOException {
    this.out = Preconditions.checkNotNull(out, "Output was null.");
    this.in = Preconditions.checkNotNull(in, "Input was null.");

    this.sourceStream = in.openStream();
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
      sourceStream.close();
    }
  }

  private void flushCopy() throws IOException {
    if (copyStart == -1) {
      return;
    }
    LOG.info(String.format("Reuse bytes from %d for %d bytes", copyStart, copyLength));

    // If the requested block is before our current position,
    // we must re-open the stream from the beginning.
    if (copyStart < currentSourceOffset) {
      if (DEBUG) {
        LOG.info(String.format("Target offset %d smaller than current offset %d, reopen stream",
            copyStart, currentSourceOffset));
      }
      sourceStream.close();
      sourceStream = in.openStream();
      currentSourceOffset = 0;
    }
    // Skip forward to the desired start.
    long toSkip = copyStart - currentSourceOffset;
    skip(toSkip);
    copy(copyLength);

    // Clear pending copy.
    copyStart = -1;
    copyLength = 0;
  }

  private void copy(long copyLength) throws IOException {
    // Now copy exactly copyLength bytes.
    byte[] buffer = new byte[8192];
    long remaining = copyLength;
    while (remaining > 0) {
      int bytesToRead = (int) Math.min(buffer.length, remaining);
      int read = sourceStream.read(buffer, 0, bytesToRead);
      if (read == -1) {
        throw new EOFException("Unexpected end of stream while copying " + copyLength
            + " bytes starting at offset " + copyStart);
      }
      out.write(buffer, 0, read);
      remaining -= read;
      currentSourceOffset += read;
    }
  }

  private void skip(long length) throws IOException {
    long remaining = length;
    while (remaining > 0) {
      long skipped = sourceStream.skip(remaining);
      if (skipped < remaining) {
        throw new EOFException("Unexpected end of stream while skipping.");
      }
      remaining -= skipped;
      currentSourceOffset += skipped;
    }
  }

}