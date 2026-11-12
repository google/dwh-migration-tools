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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import static com.google.common.base.Throwables.getCausalChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.hive.AbstractHiveConnector.HiveConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.hive.AbstractHiveConnector.ThriftClientHandle;
import com.google.edwmigration.dumper.application.dumper.connector.hive.AbstractHiveConnector.ThriftClientPool;
import com.google.edwmigration.dumper.ext.hive.metastore.HiveMetastoreThriftClient;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AbstractHiveConnectorTest {

  private static final long TEST_TIMEOUT_MILLIS = 30_000;
  private static final int STALLED_SERVER_BACKLOG = 10;

  private final HiveMetadataConnector connector = new HiveMetadataConnector();

  @Test(timeout = TEST_TIMEOUT_MILLIS)
  public void open_stalledMetastore_callTimesOut() throws Exception {
    // A listening socket that never accepts: the TCP connection succeeds, but nothing is ever
    // read or answered, like a metastore that has stopped responding.
    try (ServerSocket stalledServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      ConnectorArguments arguments =
          new ConnectorArguments(
              "--connector",
              connector.getName(),
              "--host",
              stalledServer.getInetAddress().getHostAddress(),
              "--port",
              String.valueOf(stalledServer.getLocalPort()),
              "-D" + HiveConnectorProperty.SOCKET_TIMEOUT.getName() + "=1");
      ThriftClientHandle handle = (ThriftClientHandle) connector.open(arguments);

      try (HiveMetastoreThriftClient client = handle.newClient("stalled-test-client")) {
        Exception exception = assertThrows(Exception.class, client::getAllDatabaseNames);

        assertTrue(
            "Expected a socket timeout, got: " + exception,
            getCausalChain(exception).stream().anyMatch(SocketTimeoutException.class::isInstance));
      }
    }
  }

  @Test(timeout = TEST_TIMEOUT_MILLIS)
  public void thriftClientPool_callTimedOut_nextCallUsesNewClient() throws Exception {
    try (ServerSocket stalledServer =
        new ServerSocket(0, STALLED_SERVER_BACKLOG, InetAddress.getLoopbackAddress())) {
      HiveMetastoreThriftClient.Builder builder =
          new HiveMetastoreThriftClient.Builder("2.3.6")
              .withHost(stalledServer.getInetAddress().getHostAddress())
              .withPort(stalledServer.getLocalPort())
              .withSocketTimeout(Duration.ofSeconds(1));
      List<HiveMetastoreThriftClient> usedClients = Collections.synchronizedList(new ArrayList<>());

      // A pool of size 1 runs every call on the calling thread, so both calls share one
      // thread-local client unless the first one is discarded.
      try (ThriftClientPool pool = new ThriftClientPool("timeout-test-pool", builder, 1)) {
        pool.execute(
            client -> {
              usedClients.add(client);
              try {
                client.getAllDatabaseNames();
              } catch (Exception e) {
                // Swallowed, as the per-table tasks do.
              }
            });
        pool.execute(usedClients::add);
      }

      assertEquals(2, usedClients.size());
      assertFalse(usedClients.get(0).isOpen());
      assertNotSame(usedClients.get(0), usedClients.get(1));
    }
  }

  @Test
  public void open_nonNumericTimeout_throwsUsageException() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "-D" + HiveConnectorProperty.CONNECT_TIMEOUT.getName() + "=30s");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().contains(HiveConnectorProperty.CONNECT_TIMEOUT.getName()));
  }

  @Test
  public void open_negativeTimeout_throwsUsageException() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            connector.getName(),
            "-D" + HiveConnectorProperty.SOCKET_TIMEOUT.getName() + "=-1");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.open(arguments));

    assertTrue(
        exception.getMessage(),
        exception.getMessage().contains(HiveConnectorProperty.SOCKET_TIMEOUT.getName()));
  }
}
