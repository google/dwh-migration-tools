package com.google.edwmigration.dbsync.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import javax.annotation.CheckReturnValue;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This is a variant of Adler-32 using a 2^n modulus and a constant offset.
// We could start with a=1, like Adler32, but it would complicate remove, a little.
public class RollingChecksumImpl {

  public static final HashFunction STRONG_HASH_FUNCTION = Hashing.sha256();

  private static final Logger logger = LoggerFactory.getLogger(RollingChecksumImpl.class);
  private static final boolean DEBUG = false;

  private int a, b;
  private @NonNegative int offset;
  private final byte[] block;

  public RollingChecksumImpl(@NonNegative int len) {
    this.block = new byte[len];
  }

  public @NonNegative int getBlockSize() {
    return block.length;
  }

  private static int F(byte data) {
    // Consider the difference between hash({ 0 }) and hash({ 0, 0 })
    // 31 is prime, which is nice.
    // Other magic numbers can be obtained from:
    // * private static final int 31 = magicNumberFactory.nextInt();
    // * go/bard?q=Give%20me%20a%20magic%20number%20like%20thirty%20one
    return (data + 31) & 0xFF;
  }

  /** Does not update offset. */
  private void remove(byte x) {
    int v = F(x);
    if (DEBUG)
      logger.info("Remove " + Integer.toHexString(v) + " from " + this);
    a -= v;
    b -= block.length * v;
    if (DEBUG)
      logger.info("Removed " + Integer.toHexString(v) + " from " + this);
  }

  private void add(byte x) {
    int v = F(x);
    if (DEBUG)
      logger.info("Add " + Integer.toHexString(v) + " to " + this);
    a = (a + v);
    b = (b + a);
    block[offset] = x;
    offset = (offset + 1) % block.length;
    if (DEBUG)
      logger.info("Added " + Integer.toHexString(v) + " to " + this);
  }

  // This is the canonical implementation, based on add() and remove().
  // We preserve this so that we can prove correctness of the optimized version in test.
  @VisibleForTesting
  public void reset(byte[] data, int start) {
    a = 0;
    b = 0;
    // This is not required.
    offset = 0;
    // It's not clear if this min() is desirable, or if overflow should be fatal.
    int len = Math.min(data.length - start, block.length);
    for (int i = 0; i < len; i++)
      add(data[start + i]);
    // System.arraycopy(data, start, block, 0, len);
  }

  // This is identical to the above but about 8x as fast.
  public int reset(InputStream data) throws IOException {
    int a = 0;
    int b = 0;
    int length = ByteStreams.read(data, block, 0, block.length);
    for (int i = 0, len = length; i < len; i++) {
      int v = F(block[i]);
      a = (a + v);
      b = (b + a);
    }
    this.a = a;
    this.b = b;
    this.offset = length % block.length;
    return length;
  }

  public byte roll(byte b) {
    byte prev = block[offset];
    remove(prev);
    add(b);
    return prev;
  }

  public int getWeakHashCode() {
    return (b & 0xFFFF) << 16 | a;
  }

  public HashCode getStrongHashCode() {
    Hasher hasher = STRONG_HASH_FUNCTION.newHasher();
    hasher.putBytes(block, offset, block.length - offset);
    hasher.putBytes(block, 0, offset);
    return hasher.hash();
  }

  /** Returns the most recent 'length' bytes of data. */
  public byte[] getBlock(int length) {
    if (DEBUG)
      logger.debug("Asking for {}..+{} out of {}", offset, length, Arrays.toString(block));
    Preconditions.checkPositionIndex(length, block.length, "Invalid block length.");
    byte[] out = new byte[length];
    // The length before the pointer.
    @NonNegative int len0 = Math.min(length, offset);
    @NonNegative int len1 = (length - len0);
    if (len1 > 0) {
      // Copy [?..block.length)
      if (DEBUG)
        logger.debug("Copying [{}..{})", block.length - len1, block.length);
      System.arraycopy(block, block.length - len1, out, 0, len1);
    }

    // Copy [?..offset)
    if (DEBUG)
      logger.debug("Copying [{}..{})", offset - len0, offset);
    System.arraycopy(block, offset - len0, out, len1, len0);

    if (DEBUG)
      logger.debug("Extracted {}", Arrays.toString(out));
    return out;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("a", Integer.toHexString(a))
        .add("b", Integer.toHexString(b))
        .add("offset", offset)
        .add("block", Arrays.toString(block))
        .toString();
  }
}
