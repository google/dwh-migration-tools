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
package com.google.edwmigration.dumper.ext.hive.metastore;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a Hive metastore Thrift client.
 *
 * <p>Implementations of this abstract class are not thread-safe because they wrap an underlying
 * Thrift client which itself is not thread-safe.
 */
@NotThreadSafe
public abstract class HiveMetastoreThriftClient implements AutoCloseable {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreThriftClient.class);

  public static class Builder {

    private static final Map<String, HiveMetastoreThriftClientProvider> supportedVersionMappings =
        new ImmutableMap.Builder<String, HiveMetastoreThriftClientProvider>()
            .put(
                "2.3.6",
                HiveMetastoreThriftClient_v2_3_6
                    ::new) // Matt tested locally in April 2021 with Hive v2.3.6 running atop Hadoop
            // and MySQL
            .build();

    @FunctionalInterface
    private static interface HiveMetastoreThriftClientProvider {
      HiveMetastoreThriftClient provide(@Nonnull String name, @Nonnull TProtocol protocol);
    }

    public static enum UnavailableClientVersionBehavior {
      FALLBACK,
      THROW
    }

    @Nonnull private final String requestedVersionString;
    @Nonnull private String name = "unnamed-thrift-client";
    @Nonnull private String host = "localhost";
    @Nonnegative private int port;
    @Nullable private String kerberosUrl;
    @Nonnull private String saslQop;

    @Nonnull
    private UnavailableClientVersionBehavior unavailableClientBehavior =
        UnavailableClientVersionBehavior.FALLBACK;

    private boolean debug = false;

    public Builder(@Nonnull String version) {
      this.requestedVersionString = version;
    }

    /** Copy constructor. */
    public Builder(@Nonnull Builder builder) {
      this.requestedVersionString = builder.requestedVersionString;
      this.name = builder.name;
      this.host = builder.host;
      this.port = builder.port;
      this.unavailableClientBehavior = builder.unavailableClientBehavior;
      this.debug = builder.debug;
      this.kerberosUrl = builder.kerberosUrl;
      this.saslQop = builder.saslQop;
    }

    @Nonnull
    public Builder withName(@Nonnull String name) {
      this.name = name;
      return this;
    }

    @Nonnull
    public Builder withHost(@Nonnull String host) {
      this.host = host;
      return this;
    }

    @Nonnull
    public Builder withPort(@Nonnegative int port) {
      this.port = port;
      return this;
    }

    @Nonnull
    public Builder withKerberosUrl(@Nullable String kerberosUrl) {
      this.kerberosUrl = kerberosUrl;
      return this;
    }

    @Nonnull
    public Builder withSaslQop(@Nonnull String saslQop) {
      this.saslQop = saslQop;
      return this;
    }

    @Nonnull
    public Builder withUnavailableClientVersionBehavior(
        @Nonnull UnavailableClientVersionBehavior behavior) {
      this.unavailableClientBehavior = behavior;
      return this;
    }

    @Nonnull
    public Builder withDebug(boolean value) {
      this.debug = value;
      return this;
    }

    @Nonnull
    private TTransport createTransport() throws SaslException, TTransportException {
      TTransport transport =
          new TSocket(
              new TConfiguration(
                  Integer.MAX_VALUE,
                  TConfiguration.DEFAULT_MAX_FRAME_SIZE,
                  TConfiguration.DEFAULT_RECURSION_DEPTH),
              host,
              port);

      if (kerberosUrl == null) {
        return transport;
      }

      String[] urlParts = kerberosUrl.split("/");

      if (urlParts.length != 2) {
        throw new IllegalArgumentException(
            "Please provide an URL in the format of `principal/cluster`");
      }

      Map<String, String> saslProperties = new HashMap<>();
      saslProperties.put(Sasl.SERVER_AUTH, "true");
      saslProperties.put(Sasl.QOP, saslQop);

      // See:
      // https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/single-signon.html
      return new TSaslClientTransport(
          "GSSAPI", null, urlParts[0], urlParts[1], saslProperties, null, transport);
    }

    @Nonnull
    public HiveMetastoreThriftClient build() throws TTransportException, SaslException {
      TTransport transport = createTransport();

      TProtocol protocol = new TBinaryProtocol(transport);
      transport.open();

      final HiveMetastoreThriftClient client;
      if (supportedVersionMappings.containsKey(requestedVersionString)) {
        if (debug)
          LOG.debug(
              "The request for Hive metastore Thrift client version '{}' is satisfiable; building"
                  + " it now.",
              requestedVersionString);
        client = supportedVersionMappings.get(requestedVersionString).provide(name, protocol);
      } else {
        String messagePrefix =
            "The request for Hive metastore Thrift client version '"
                + requestedVersionString
                + "' is NOT satisfiable. "
                + "Available versions are: ["
                + Joiner.on(",").join(supportedVersionMappings.keySet())
                + "].";
        if (unavailableClientBehavior == UnavailableClientVersionBehavior.FALLBACK) {
          LOG.warn(
              messagePrefix
                  + " The caller requested fallback behavior, so a client compiled against a"
                  + " superset Thrift specification will be used instead. If you encounter an error"
                  + " when using the fallback client, please contact CompilerWorks support and"
                  + " provide the originally requested version number.");
          client = new HiveMetastoreThriftClient_Superset(name, protocol);
        } else {
          throw new UnsupportedOperationException(
              messagePrefix
                  + " Aborting now; the caller indicated that this is an irrecoverable condition.");
        }
      }

      return client;
    }
  }

  @Nonnull private final String name;

  public HiveMetastoreThriftClient(@Nonnull String name) {
    this.name = Preconditions.checkNotNull(name, "name was null.");
  }

  @Nonnull
  public String getName() {
    return name;
  }

  @Nonnull
  public abstract List<? extends String> getAllDatabaseNames() throws Exception;

  @Nonnull
  public abstract Database getDatabase(@Nonnull String databaseName) throws Exception;

  @Nonnull
  public abstract List<? extends String> getAllTableNamesInDatabase(@Nonnull String databaseName)
      throws Exception;

  @Nonnull
  public abstract Table getTable(@Nonnull String databaseName, @Nonnull String tableName)
      throws Exception;

  @Nonnull
  public abstract List<? extends Function> getFunctions() throws Exception;
}
