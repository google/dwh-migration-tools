package com.google.edwmigration.dbsync.common;

import com.google.common.base.Optional;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dbsync.proto.Checksum;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChecksumGenerator {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(ChecksumGenerator.class);

  private static final boolean DEBUG = false;

  private final @NonNegative int blockSize;

  public ChecksumGenerator(@NonNegative int blockSize) {
    this.blockSize = blockSize;
  }

  public @NonNegative int getBlockSize() {
    return blockSize;
  }

  public void generate(ChecksumConsumer<IOException> out, ByteSource in) throws IOException {
    // This deliberately throws if size is not Present.
    Optional<Long> dataSizeOptional = in.sizeIfKnown();
    if (!dataSizeOptional.isPresent()) {
      throw new IllegalArgumentException("ServerData must have a known size.");
    }
    long dataSize = dataSizeOptional.get();
    byte[] dataArray = DEBUG ? in.read() : null;
    RollingChecksumImpl rollingChecksum = new RollingChecksumImpl(blockSize);
    try (InputStream i = in.openStream()) {
      for (long offset = 0; offset < dataSize; offset += blockSize) {
        // This can happen on the last iteration.
        long remaining = dataSize - offset;
        if (remaining < blockSize) {
          // This cast is safe because dataSize - offset is nonnegative, and blockSize is an int.
          rollingChecksum = new RollingChecksumImpl(Ints.checkedCast(remaining));
        }
        // Yes, this masks the instance variable.
        int blockSize = rollingChecksum.getBlockSize();
        if (DEBUG) {
          logger.info(
              "Generating checksums for [{} .. +{}] in {} bytes", offset, blockSize, dataSize);
        }

        // If someone changes the size of the ByteSource underneath us, this might throw
        // EOFException.
        rollingChecksum.reset(i);
        int weakHashCode = rollingChecksum.getWeakHashCode();
        HashCode strongHashCode = rollingChecksum.getStrongHashCode();

        if (DEBUG) {
          HashCode _strongHashCode =
              RollingChecksumImpl.STRONG_HASH_FUNCTION.hashBytes(
                  dataArray, Ints.checkedCast(offset), blockSize);
          if (!strongHashCode.equals(_strongHashCode)) {
            throw new IllegalStateException(
                "Bad hash code at "
                    + offset
                    + "..+"
                    + blockSize
                    + ": "
                    + strongHashCode
                    + " != "
                    + _strongHashCode);
          }
        }

        Checksum c =
            Checksum.newBuilder()
                .setBlockOffset(offset)
                .setBlockLength(blockSize)
                .setWeakChecksum(weakHashCode)
                .setStrongChecksum(ByteString.copyFrom(strongHashCode.asBytes()))
                .build();
        out.accept(c);
      }
    }
  }

  public interface ChecksumConsumer<X extends Exception> {

    void accept(Checksum checksum) throws X;
  }
}
