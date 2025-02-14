package com.google.edwmigration.dbsync.common;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.longs.LongHash;
import java.util.Arrays;

public class DefaultHashStrategies {

  public static final Int32 INSTANCE32 = new Int32();
  public static final Int64 INSTANCE64 = new Int64();
  public static final ByteArray BYTE_ARRAY = new ByteArray();

  /**
   * Based on Thomas Wang's code from https://gist.github.com/badboy/6267743
   */
  @SuppressWarnings("UnnecessaryParentheses")
  public static int hash_mix_64_to_32(long key) {
    key = (~key) + (key << 18); // key = (key << 18) - key - 1;
    key = key ^ (key >>> 31);
    key = key * 21; // key = (key + (key << 2)) + (key << 4);
    key = key ^ (key >>> 11);
    key = key + (key << 6);
    key = key ^ (key >>> 22);
    return (int) key;
  }

  /**
   * Based on http://www.burtleburtle.net/bob/hash/integer.html .
   */
  public static int hash_mix_full(int a) {
    a = (a + 0x7ed55d16) + (a << 12);
    a = (a ^ 0xc761c23c) ^ (a >> 19);
    a = (a + 0x165667b1) + (a << 5);
    a = (a + 0xd3a2646c) ^ (a << 9);
    a = (a + 0xfd7046c5) + (a << 3);
    a = (a ^ 0xb55a4f09) ^ (a >> 16);
    return a;
  }

  public static int hash_mix_full(long a) {
    return hash_mix_full(Long.hashCode(a));
  }

  /**
   * Based on http://www.burtleburtle.net/bob/hash/integer.html .
   */
  public static int hash_mix_low(int a) {
    a += ~(a << 15);
    a ^= (a >> 10);
    a += (a << 3);
    a ^= (a >> 6);
    a += ~(a << 11);
    a ^= (a >> 16);
    return a;
  }

  public static int hash_mix_low(long a) {
    return hash_mix_low(Long.hashCode(a));
  }

  public static class Int32 implements IntHash.Strategy {

    @Override
    public int hashCode(int e) {
      return hash_mix_full(e);
    }

    @Override
    public boolean equals(int a, int b) {
      return a == b;
    }
  }

  public static class Int64 implements LongHash.Strategy {

    @Override
    public int hashCode(long e) {
      return hash_mix_full(e);
    }

    @Override
    public boolean equals(long a, long b) {
      return a == b;
    }
  }

  public static class ByteArray implements Hash.Strategy<byte[]> {

    @Override
    public int hashCode(byte[] o) {
      return Arrays.hashCode(o);
    }

    @Override
    public boolean equals(byte[] a, byte[] b) {
      return Arrays.equals(a, b);
    }
  }
}
