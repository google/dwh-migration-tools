/*
 * Copyright 2022-2023 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.edwmigration.dumper.common;

import com.google.common.base.Ticker;
import com.google.common.primitives.Longs;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates type-1 globally-unique UUIDs.
 *
 * @author shevek
 */
public class UUIDGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(UUIDGenerator.class);

  private static class Inner {

    private static final UUIDGenerator INSTANCE = new UUIDGenerator(Ticker.systemTicker());
  }

  @Nonnull
  public static UUIDGenerator getInstance() {
    return Inner.INSTANCE;
  }

  /**
   * Attempts to return a hardware address from a "useful" interface on this system.
   *
   * <p>Whether this method returns null or throws SocketException on failure is not especially
   * well-defined.
   *
   * @return A hardware address or null on (some forms of) error.
   * @throws SocketException on other forms of error.
   */
  @CheckForNull
  public static byte[] getMacOrNull() throws SocketException {
    Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
    if (ifaces == null) // This happens in no-network jails.
    return null;

    for (NetworkInterface iface : Collections.list(ifaces)) {
      if (iface.isLoopback()) continue;
      if (iface.isPointToPoint()) continue;
      if (iface.isVirtual()) continue;
      for (InetAddress addr : Collections.list(iface.getInetAddresses())) {
        if (addr.isAnyLocalAddress()) continue;
        if (addr.isLinkLocalAddress()) continue;
        if (addr.isLoopbackAddress()) continue;
        if (addr.isMulticastAddress()) continue;
        byte[] hwaddr = iface.getHardwareAddress();
        if (ArrayUtils.isEmpty(hwaddr)) continue;
        return Arrays.copyOf(hwaddr, 6);
      }
    }

    return null;
  }

  @Nonnull
  private static byte[] getMac() {
    IFACE:
    try {
      byte[] data = getMacOrNull();
      if (data != null) return data;
      break IFACE;
    } catch (Exception e) {
      // Notionally, this is an IOException, but it might also be an (unexpected) SecurityException
      // or a NullPointerException if some security-aware component returned null instead of real
      // data.
      LOG.warn("Failed to get MAC from NetworkInterface address: " + e, e);
    }

    byte[] data = new byte[6];
    Random r = new SecureRandom();
    r.nextBytes(data);
    return data;
  }

  @Nonnull private final Ticker ticker;
  private final byte[] mac = getMac();
  private final long macWord =
      Longs.fromBytes((byte) 0, (byte) 0, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  private final long initMillis = System.currentTimeMillis();
  private final long initNanos;
  private final AtomicInteger seq = new AtomicInteger();

  public UUIDGenerator(@Nonnull Ticker ticker) {
    this.ticker = ticker;
    this.initNanos = ticker.read();
  }

  private long newTime() {
    long deltaNanos = ticker.read() - initNanos;
    long time = (initMillis * 1000 * 10) + (deltaNanos / 100);
    // LOG.info("Time is           " + time);
    return time;
  }

  private long newMsw() {
    long word = 0;
    // version = 1
    word |= 1 << 12;

    long time = newTime();
    word |= (time >>> 48) & 0x0FFFL;
    word |= ((time >>> 32) & 0xFFFFL) << 16;
    word |= (time & 0xFFFFFFFFL) << 32;
    return word;
  }

  private long newLsw() {
    long word = 0;
    // variant
    word |= 2L << 62;
    // sequence
    word |= (seq.getAndIncrement() & 0x3FFFL) << 48;
    // mac
    word |= macWord;
    return word;
  }

  @Nonnull
  public UUID nextUUID() {
    return new UUID(newMsw(), newLsw());
  }

  @Nonnull
  public static byte[] toBytes(long msw, long lsw) {
    byte[] out = new byte[16];

    for (int i = 7; i >= 0; i--) {
      out[i] = (byte) (msw & 0xFFL);
      msw >>>= 8;
    }

    for (int i = 7; i >= 0; i--) {
      out[i + 8] = (byte) (lsw & 0xFFL);
      lsw >>>= 8;
    }

    return out;
  }

  @Nonnull
  public byte[] nextBytes() {
    return toBytes(newMsw(), newLsw());
  }
}
