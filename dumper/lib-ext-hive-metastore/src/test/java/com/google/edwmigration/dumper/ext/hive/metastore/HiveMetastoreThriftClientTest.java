/*
 * Copyright 2022-2025 Google LLC
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
package com.google.edwmigration.dumper.ext.hive.metastore;

import static com.google.common.base.Throwables.getCausalChain;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HiveMetastoreThriftClientTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger logger = LoggerFactory.getLogger(HiveMetastoreThriftClientTest.class);

  @Ignore("For local execution only; Jenkins doesn't have a live Hive metastore running.")
  @Test
  public void testClientAgainstLiveMetastore_v2_3_6() throws Exception {
    HiveMetastoreThriftClient client =
        new HiveMetastoreThriftClient.Builder("2.3.6").withHost("localhost").withPort(9083).build();
    ImmutableList<String> databaseNames = client.getAllDatabaseNames();
    logger.info("Databases in metastore: {}", databaseNames);
    assertTrue("Expected at least one database name.", databaseNames.size() > 0);
  }

  /**
   * Ensures that we fallback to the superset Thrift specification when we request a client for a
   * version which is unavailable or does not exist.
   */
  @Ignore("For local execution only; Jenkins doesn't have a live Hive metastore running.")
  @Test
  public void testClientAgainstLiveMetastore_Fallback() throws Exception {
    HiveMetastoreThriftClient client =
        new HiveMetastoreThriftClient.Builder("100.200.300")
            .withHost("localhost")
            .withPort(9083)
            .build();
    ImmutableList<String> databaseNames = client.getAllDatabaseNames();
    logger.info("Databases in metastore: {}", databaseNames);
    assertTrue("Expected at least one database name.", databaseNames.size() > 0);
  }

  @Test(timeout = 30_000)
  public void build_stalledMetastore_callTimesOut() throws Exception {
    // A listening socket that never accepts: the TCP connection succeeds, but nothing is ever
    // read or answered, like a metastore that has stopped responding.
    try (ServerSocket stalledServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      HiveMetastoreThriftClient.Builder builder =
          new HiveMetastoreThriftClient.Builder("2.3.6")
              .withHost(stalledServer.getInetAddress().getHostAddress())
              .withPort(stalledServer.getLocalPort())
              .withSocketTimeout(Duration.ofSeconds(1));

      // Clients are built from a copy of a configured builder, so the copy must keep the timeout.
      try (HiveMetastoreThriftClient client =
          new HiveMetastoreThriftClient.Builder(builder).build()) {
        Exception exception = assertThrows(Exception.class, client::getAllDatabaseNames);

        assertTrue(
            "Expected a socket timeout, got: " + exception,
            getCausalChain(exception).stream().anyMatch(SocketTimeoutException.class::isInstance));
        assertFalse("A client whose call timed out must be closed.", client.isOpen());
      }
    }
  }

  @Test
  public void withSocketTimeout_negative_throws() {
    HiveMetastoreThriftClient.Builder builder = new HiveMetastoreThriftClient.Builder("2.3.6");

    assertThrows(
        IllegalArgumentException.class, () -> builder.withSocketTimeout(Duration.ofSeconds(-1)));
  }

  @Test
  public void withConnectTimeout_beyondIntMillis_throws() {
    HiveMetastoreThriftClient.Builder builder = new HiveMetastoreThriftClient.Builder("2.3.6");
    Duration tooLong = Duration.ofMillis(Integer.MAX_VALUE).plusMillis(1);

    assertThrows(IllegalArgumentException.class, () -> builder.withConnectTimeout(tooLong));
  }
}
