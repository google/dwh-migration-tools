/*
 * Copyright 2022 Google LLC
 * Copyright 2013-2021 CompilerWorks
 * Copyright (C) 2015-2021 Expedia, Inc. and Klarna AB.
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
package com.google.edwmigration.dumper.application.dumper.connector.hive.support;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORE_FASTPATH;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge23;
import org.apache.hive.service.Service.STATE;
import org.apache.hive.service.server.HiveServer2;
import org.checkerframework.checker.index.qual.Positive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support class to run a Hive Metastore listening on thrift.
 */
// This class contains some code sourced from and inspired by HiveRunner and BeeJU, specifically
// https://github.com/klarna/HiveRunner/blob/fb00a98f37abdb779547c1c98ef6fbe54d373e0c/src/main/java/com/klarna/hiverunner/StandaloneHiveServerContext.java
// https://github.com/ExpediaGroup/beeju/blob/a3e821b9bdb70f0e603cccb6408c319b241df66c/src/main/java/com/hotels/beeju/core/BeejuCore.java
public class HiveServerSupport implements AutoCloseable {

  public static final int CONCURRENCY = 32; // Default is min:5 max:500
  private final static Logger LOG = LoggerFactory.getLogger(HiveServerSupport.class);

  // "user" conflicts with USER db and the metastore_db can't be created.
  private static final String METASTORE_DB_USER = "db_user";
  private static final String METASTORE_DB_PASSWORD = "db_password";

  private final HiveConf conf = new HiveConf();
  private final int thriftPortMetastore;
  private final int thriftPortServer;

  private Path hiveTestDir;
  private HiveServer2 hiveServer;
  private ExecutorService metastoreExecutor;

  public HiveServerSupport() throws IOException {
    thriftPortServer = getFreePort();
    thriftPortMetastore = getFreePort();
    configure();
  }

  @Positive
  public int getMetastoreThriftPort() {
    return thriftPortMetastore;
  }

  @SuppressWarnings("deprecation")
  public void configure() {
    try {
      hiveTestDir = Files.createTempDirectory("dumper-hive-test-");

      createAndSetFolderProperty(HiveConf.ConfVars.SCRATCHDIR, "scratchdir");
      createAndSetFolderProperty(HiveConf.ConfVars.LOCALSCRATCHDIR, "localscratchdir");
      createAndSetFolderProperty(HiveConf.ConfVars.HIVEHISTORYFILELOC, "hivehistoryfileloc");

      Path derbyHome = Files.createTempDirectory(hiveTestDir, "derby-home-");
      System.setProperty("derby.system.home", derbyHome.toString());

      String derbyLog = Files.createTempFile(hiveTestDir, "derby", ".log").toString();
      System.setProperty("derby.stream.error.file", derbyLog);

      Path warehouseDir = Files.createTempDirectory(hiveTestDir, "hive-warehouse-");
      setHiveVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());

      String driverClassName = EmbeddedDriver.class.getName();
      conf.setBoolean("hcatalog.hive.client.cache.disabled", true);
      String connectionURL = "jdbc:derby:memory:" + UUID.randomUUID() + ";create=true";

      setMetastoreAndSystemProperty(ConfVars.CONNECT_URL_KEY, connectionURL);
      setMetastoreAndSystemProperty(ConfVars.CONNECTION_DRIVER, driverClassName);
      setMetastoreAndSystemProperty(ConfVars.CONNECTION_USER_NAME, METASTORE_DB_USER);
      setMetastoreAndSystemProperty(ConfVars.PWD, METASTORE_DB_PASSWORD);

      conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "NONE");
      conf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, true);

      setMetastoreAndSystemProperty(ConfVars.AUTO_CREATE_ALL, "true");
      setMetastoreAndSystemProperty(ConfVars.SCHEMA_VERIFICATION, "false");

      conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, 0); // disable
      conf.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
      setMetastoreAndSystemProperty(ConfVars.EVENT_DB_NOTIFICATION_API_AUTH, "false");
      conf.setTimeVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, 0,
          TimeUnit.MILLISECONDS);
      conf.set(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname, "DUMMY");
      System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_MATERIALIZED_VIEWS_REGISTRY_IMPL.varname,
          "DUMMY");

      conf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, true); // Do not print "OK"
      setHiveVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + thriftPortMetastore);
      setHiveIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, thriftPortServer);

    } catch (IOException e) {
      throw new UncheckedIOException("Error during configuration.", e);
    }
  }

  public void startMetastore() throws InterruptedException {
    metastoreExecutor = Executors.newFixedThreadPool(CONCURRENCY);

    final Lock startLock = new ReentrantLock();
    final Condition startCondition = startLock.newCondition();
    final AtomicBoolean startedServing = new AtomicBoolean();

    final HiveConf hiveConf = new HiveConf(conf, HiveMetaStoreClient.class);
    metastoreExecutor.execute(() -> {
      try {
        HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge23.getBridge();
        HiveMetaStore.startMetaStore(thriftPortMetastore, bridge, hiveConf, startLock,
            startCondition, startedServing);
      } catch (Throwable t) {
        LOG.error("Unable to start Hive Metastore", t);
      }
    });

    wait(startLock, startCondition);

  }

  public void startServer() throws InterruptedException {
    hiveServer = new HiveServer2();
    hiveServer.init(conf);
    hiveServer.start();
    waitForState(hiveServer, STATE.STARTED);

  }

  public HiveServerSupport start() throws InterruptedException {
    LOG.info("Starting Hive Metastore on port {}, HiveServer2 on port {} ...",
        thriftPortMetastore, thriftPortServer);
    conf.setBoolVar(METASTORE_FASTPATH, true);
    startMetastore();
    startServer();
    LOG.info("Started.");
    return this;
  }

  public void execute(String... sqlScripts) throws SQLException {
    try (Connection connection = getJdbcConnection(); Statement statement = connection.createStatement()) {
      for (String script : sqlScripts) {
        statement.execute(script);
      }
    }
  }

  public Connection getJdbcConnection() throws SQLException {
    String jdbcConnectionUrl = "jdbc:hive2://localhost:" + thriftPortServer;
    return DriverManager.getConnection(jdbcConnectionUrl);
  }

  private int getFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private void waitForState(HiveServer2 hiveServer, STATE targetState) throws InterruptedException {
    int retries = 0;
    int maxRetries = 5;
    while (hiveServer.getServiceState() != targetState && retries < maxRetries) {
      TimeUnit.SECONDS.sleep(1);
      retries++;
    }
    if (retries >= maxRetries) {
      throw new RuntimeException("HiveServer2 did not start in a reasonable time");
    }
  }

  private void wait(Lock startLock, Condition startCondition) throws InterruptedException {
    for (int j = 0; j < 3; j++) {
      startLock.lock();
      try {
        if (startCondition.await(1, TimeUnit.MINUTES)) {
          return;
        }
      } finally {
        startLock.unlock();
      }
    }
    throw new RuntimeException(
        "Maximum number of tries reached whilst waiting for Thrift server to be ready");
  }

  private void setMetastoreAndSystemProperty(ConfVars key, String value) {
    conf.set(key.getVarname(), value);
    conf.set(key.getHiveName(), value);

    System.setProperty(key.getVarname(), value);
    System.setProperty(key.getHiveName(), value);
  }

  private void createAndSetFolderProperty(HiveConf.ConfVars var, String childFolderName)
      throws IOException {
    String folderPath = newFolder(hiveTestDir, childFolderName).toAbsolutePath().toString();
    conf.setVar(var, folderPath);
  }

  private Path newFolder(Path basedir, String folder) throws IOException {
    Path newFolder = Files.createTempDirectory(basedir, folder);
    FileUtil.setPermission(newFolder.toFile(), FsPermission.getDirDefault());
    return newFolder;
  }

  public void setHiveVar(HiveConf.ConfVars variable, String value) {
    conf.setVar(variable, value);
  }

  public void setHiveIntVar(HiveConf.ConfVars variable, int value) {
    conf.setIntVar(variable, value);
  }

  @Override
  public void close() throws Exception {
    hiveServer.stop();
    waitForState(hiveServer, STATE.STOPPED);
    MoreExecutors.shutdownAndAwaitTermination(metastoreExecutor, 30, TimeUnit.SECONDS);
    FileUtils.deleteDirectory(hiveTestDir.toFile());
  }
}
