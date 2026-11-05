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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner.SqlQuoting.quoteIdentifier;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.handle.HandleUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * Executes SQL statements on a running all-purpose cluster or SQL warehouse with the Databricks
 * JDBC driver, through its Thrift endpoint, without CloudFetch external result links. Up to {@code
 * maxConnections} statements run at a time on a {@link HikariDataSource} pool, each on its own
 * connection and so its own session. The connections stay open in the pool for later statements
 * until {@link #close()}, and each {@link StatementResult} streams rows from its {@link ResultSet}
 * and returns its connection to the pool when closed. The catalog of a connection is set with
 * {@code USE CATALOG} when it changes. Like the pools of the other JDBC connectors, the pool opens
 * and closes a connection when it is created, so that a wrong token, ID or host fails right away.
 *
 * <p>The driver ({@code com.databricks:databricks-jdbc}) is built for Java 11, so it is a runtime
 * dependency that is loaded by name only when JDBC is selected, and the dumper still compiles and
 * runs on Java 8 without it. On Java 16 and above, the dumper scripts set the JVM option {@code
 * --add-opens=java.base/java.nio=ALL-UNNAMED} for Apache Arrow, though driver 3.4.4 also ran
 * without it on Java 21.
 */
@ThreadSafe
public final class JdbcSqlRunner implements DatabricksSqlRunner {

  private static final String DRIVER_CLASS_NAME = "com.databricks.client.jdbc.Driver";
  // Uses the Thrift protocol directly (UseThriftClient=1) and disables CloudFetch external links
  // (EnableQueryResultDownload=0), with token auth and without driver telemetry.
  private static final String URL_FORMAT =
      "jdbc:databricks://%s:%d/default;transportMode=http;ssl=%d;httpPath=%s;"
          + "AuthMech=3;EnableTelemetry=0;UseThriftClient=1;EnableQueryResultDownload=0";
  private static final String WAREHOUSE_QUERY_TAGS =
      ";query_tags="
          + DatabricksClientSqlRunner.QUERY_TAG_KEY
          + ":"
          + DatabricksClientSqlRunner.QUERY_TAG_VALUE;
  private static final int HTTPS_PORT = 443;
  private static final int HTTP_PORT = 80;
  private static final String BEARER_PREFIX = "Bearer ";
  // A statement fails after waiting this long for a connection, such as when the network is down,
  // instead of blocking forever. Connections usually open in seconds.
  private static final long CONNECTION_TIMEOUT_MILLIS = MINUTES.toMillis(5);

  private final HikariDataSource dataSource;
  private final Map<Connection, String> connectionCatalogs =
      Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Creates the runner of the cluster {@code clusterId} of the workspace of {@code
   * workspaceClient}, whose token authenticates the connections, with up to {@code maxConnections}
   * connections. Throws an IOException with the error of the driver when no connection opens.
   */
  public static JdbcSqlRunner forCluster(
      WorkspaceClient workspaceClient, String clusterId, int maxConnections) throws IOException {
    Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    Preconditions.checkNotNull(clusterId, "Cluster ID was null.");
    Preconditions.checkArgument(maxConnections > 0, "Max connections was %s.", maxConnections);
    DatabricksConfig config = workspaceClient.config();
    return new JdbcSqlRunner(config, createClusterUrl(config, clusterId), maxConnections);
  }

  /**
   * Creates the runner of the SQL warehouse {@code warehouseId} of the workspace of {@code
   * workspaceClient}, whose token authenticates the connections, with up to {@code maxConnections}
   * connections. Throws an IOException with the error of the driver when no connection opens.
   */
  public static JdbcSqlRunner forWarehouse(
      WorkspaceClient workspaceClient, String warehouseId, int maxConnections) throws IOException {
    Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    Preconditions.checkNotNull(warehouseId, "Warehouse ID was null.");
    Preconditions.checkArgument(maxConnections > 0, "Max connections was %s.", maxConnections);
    DatabricksConfig config = workspaceClient.config();
    return new JdbcSqlRunner(config, createWarehouseUrl(config, warehouseId), maxConnections);
  }

  private JdbcSqlRunner(DatabricksConfig databricksConfig, String url, int maxConnections)
      throws IOException {
    Driver driver = loadDriver();
    HikariConfig config =
        HandleUtil.createHikariConfig(
            new SimpleDriverDataSource() {
              @Override
              public Connection getConnection() throws SQLException {
                return openConnection(driver, databricksConfig, url);
              }

              // Ignores the login timeout, which the opener doesn't use, instead of throwing like
              // the base class, which the pool would log as a failure.
              @Override
              public void setLoginTimeout(int timeout) {}
            },
            maxConnections);
    config.setConnectionTimeout(CONNECTION_TIMEOUT_MILLIS);
    try {
      this.dataSource = new HikariDataSource(config);
    } catch (PoolInitializationException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new IOException(
          "Failed to open a Databricks JDBC connection: " + cause.getMessage(), cause);
    }
  }

  /** Returns the JDBC URL of the Thrift endpoint of a cluster. */
  private static String createClusterUrl(DatabricksConfig config, String clusterId)
      throws IOException {
    URI host = parseHost(config);
    return formatUrl(
        host, String.format("sql/protocolv1/o/%s/%s", getWorkspaceId(config, host), clusterId));
  }

  /**
   * Returns the JDBC URL of the Thrift endpoint of a SQL warehouse, with the dumper query tag so
   * query history dumps skip the statements.
   */
  private static String createWarehouseUrl(DatabricksConfig config, String warehouseId)
      throws IOException {
    return formatUrl(parseHost(config), "sql/1.0/warehouses/" + warehouseId) + WAREHOUSE_QUERY_TAGS;
  }

  private static URI parseHost(DatabricksConfig config) throws IOException {
    URI host;
    try {
      host = new URI(Strings.nullToEmpty(config.getHost()));
    } catch (URISyntaxException e) {
      throw new IOException("Invalid workspace URL: " + config.getHost(), e);
    }

    if (host.getHost() == null) {
      throw new IOException("The workspace URL has no host: " + config.getHost());
    }

    return host;
  }

  private static String formatUrl(URI host, String httpPath) {
    boolean ssl = !"http".equalsIgnoreCase(host.getScheme());
    int port = host.getPort() != -1 ? host.getPort() : ssl ? HTTPS_PORT : HTTP_PORT;
    return String.format(URL_FORMAT, host.getHost(), port, ssl ? 1 : 0, httpPath);
  }

  private static String getWorkspaceId(DatabricksConfig config, URI host) throws IOException {
    if (Strings.isNullOrEmpty(config.getWorkspaceId())) {
      throw new IOException(
          String.format(
              "Can't find the workspace ID of %s for the Thrift endpoint of the cluster. Set the"
                  + " DATABRICKS_WORKSPACE_ID environment variable.",
              host.getHost()));
    }

    return config.getWorkspaceId();
  }

  /**
   * Returns a new instance of the Databricks JDBC driver, or throws a usage error on Java 8, which
   * can't load the Java 11 classes of the driver.
   */
  private static Driver loadDriver() throws IOException {
    try {
      return Class.forName(DRIVER_CLASS_NAME)
          .asSubclass(Driver.class)
          .getDeclaredConstructor()
          .newInstance();
    } catch (UnsupportedClassVersionError e) {
      throw new MetadataDumperUsageException(
          String.format(
              "The Databricks JDBC driver requires Java 11 or above, but the dumper is running on"
                  + " Java %s. Please run the dumper with Java 11 or above. On Java 8, use"
                  + " '--warehouse <warehouse-id>' without the 'jdbc' warehouse transport.",
              System.getProperty("java.specification.version")));
    } catch (ReflectiveOperationException e) {
      throw new IOException("Failed to load the Databricks JDBC driver: " + DRIVER_CLASS_NAME, e);
    }
  }

  /** Opens a connection with the current token of the SDK, which can be a refreshed OAuth token. */
  private static Connection openConnection(Driver driver, DatabricksConfig config, String url)
      throws SQLException {
    String authorization = Strings.nullToEmpty(config.authenticate().get("Authorization"));
    if (!authorization.startsWith(BEARER_PREFIX)) {
      throw new SQLException(
          "The Databricks JDBC connection needs token authentication, such as a personal access"
              + " token or OAuth.");
    }

    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", authorization.substring(BEARER_PREFIX.length()));
    Connection connection = driver.connect(url, properties);
    if (connection == null) {
      throw new SQLException("The Databricks JDBC driver doesn't accept the URL " + url);
    }

    return connection;
  }

  @Override
  public StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      connection = this.dataSource.getConnection();
      statement = connection.createStatement();
      Connection rawConnection = connection.unwrap(Connection.class);
      if (!catalog.equals(this.connectionCatalogs.get(rawConnection))) {
        statement.execute("USE CATALOG " + quoteIdentifier(catalog));
        this.connectionCatalogs.put(rawConnection, catalog);
      }

      if (!statement.execute(sql)) {
        Connection toCloseConnection = connection;
        Statement toCloseStatement = statement;
        connection = null;
        statement = null;
        closeAll(toCloseConnection, toCloseStatement, null);
        return EmptyStatementResult.INSTANCE;
      }

      resultSet = statement.getResultSet();
      return new JdbcStatementResult(connection, statement, resultSet);
    } catch (SQLException e) {
      closeQuietly(resultSet);
      closeQuietly(statement);
      closeQuietly(connection);
      throw toIoException(e);
    } catch (RuntimeException | Error e) {
      closeQuietly(resultSet);
      closeQuietly(statement);
      closeQuietly(connection);
      throw e;
    }
  }

  @Override
  public void close() {
    this.dataSource.close();
  }

  private static IOException toIoException(SQLException e) {
    return new IOException(
        String.format("SQL statement failed (SQLSTATE %s): %s", e.getSQLState(), e.getMessage()),
        e);
  }

  private static void closeAll(Connection connection, Statement statement, ResultSet resultSet)
      throws IOException {
    try (Connection c = connection;
        Statement s = statement;
        ResultSet r = resultSet) {
      // Closed in reverse order: ResultSet, Statement, Connection.
    } catch (SQLException e) {
      throw toIoException(e);
    }
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (Exception ignored) {
        // Keep the primary failure.
      }
    }
  }

  private enum EmptyStatementResult implements StatementResult {
    INSTANCE;

    @Override
    public ImmutableList<String> columnNames() {
      return ImmutableList.of();
    }

    @Override
    public List<String> nextRow() {
      return null;
    }
  }

  /** Streams rows directly from a JDBC {@link ResultSet} and closes its connection on close. */
  private static final class JdbcStatementResult implements StatementResult {

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final ImmutableList<String> columnNames;
    private boolean closed;

    private JdbcStatementResult(Connection connection, Statement statement, ResultSet resultSet)
        throws SQLException {
      this.connection = connection;
      this.statement = statement;
      this.resultSet = resultSet;
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      ImmutableList.Builder<String> names = ImmutableList.builder();
      for (int column = 1; column <= columnCount; column++) {
        names.add(metaData.getColumnLabel(column));
      }
      this.columnNames = names.build();
    }

    @Override
    public ImmutableList<String> columnNames() {
      return this.columnNames;
    }

    @Override
    public List<String> nextRow() throws IOException {
      try {
        if (!this.resultSet.next()) {
          return null;
        }

        int columnCount = this.columnNames.size();
        List<String> row = new ArrayList<>(columnCount);
        for (int column = 1; column <= columnCount; column++) {
          row.add(this.resultSet.getString(column));
        }

        // The cells can be null, which ImmutableList doesn't allow.
        return Collections.unmodifiableList(row);
      } catch (SQLException e) {
        throw toIoException(e);
      }
    }

    @Override
    public void close() throws IOException {
      if (!this.closed) {
        this.closed = true;
        closeAll(this.connection, this.statement, this.resultSet);
      }
    }
  }
}
