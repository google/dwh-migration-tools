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
package com.google.edwmigration.dumper.application.dumper.connector.hive;

import com.google.common.collect.Lists;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import com.google.edwmigration.dumper.application.dumper.connector.hive.support.HiveServerSupport;
import com.google.edwmigration.dumper.application.dumper.connector.hive.support.HiveTestSchemaBuilder;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ConcurrentRecordProgressMonitor;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run this with: > ./gradlew :dumper:app:{cleanTest,test} --tests HiveMetadataConnectorTest
 * -Dtest.verbose=true -Dorg.gradle.java.home=/usr/lib/jvm/java-1.8.0-openjdk-amd64
 */
@RunWith(JUnit4.class)
public class HiveMetadataConnectorTest extends AbstractConnectorTest {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadataConnectorTest.class);
  private static final boolean debug = false;

  private final HiveMetadataConnector connector = new HiveMetadataConnector();

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  @Test
  public void testLoadedHive312() throws Exception {

    // Hive 3.1.2 requires java 1.8
    Assume.assumeTrue(SystemUtils.IS_JAVA_1_8);

    // with 5/5/10/1000 -> ~2 minutes
    int NUM_SCHEMAS = 5;
    int NUM_TABLES = 5;
    int NUM_COLUMNS = 10;
    int NUM_PARTITIONS = 1000;

    int NUM_DUMPER_RUNS = 5; // 5 -> ~4 minutes
    int BATCH_SIZE = 25;

    List<List<String>> setupStatements =
        HiveTestSchemaBuilder.getStatements(NUM_SCHEMAS, NUM_TABLES, NUM_COLUMNS, NUM_PARTITIONS);

    try (HiveServerSupport instanceSupport = new HiveServerSupport().start()) {

      // Populate Hive Metastore
      long total = setupStatements.stream().mapToLong(Collection::size).sum();
      try (ConcurrentProgressMonitor monitor =
          new ConcurrentRecordProgressMonitor("Populating Hive instance.", total)) {
        ExecutorService executor = Executors.newFixedThreadPool(HiveServerSupport.CONCURRENCY);

        for (List<String> statementList : setupStatements) {
          executor
              .invokeAll(
                  Lists.partition(statementList, BATCH_SIZE).stream()
                      .map(l -> getCallable(instanceSupport, l, monitor))
                      .collect(Collectors.toList()))
              .forEach(this::assertNoException);
        }
      }

      // Run dumper many times and assert all tables are there (1 table = 1 line, JSONL)
      for (int i = 0; i < NUM_DUMPER_RUNS; i++) {
        String tmpPrefix = String.format("dumper-test-iteration-%d-", i);
        Path tmpPath = Files.createTempFile(tmpPrefix, ".zip");
        File tmpFile = tmpPath.toFile();

        try {
          MetadataDumper.main(
              "--connector",
              "hiveql",
              "--port",
              "" + instanceSupport.getMetastoreThriftPort(),
              "--output",
              tmpFile.getAbsolutePath());

          Assert.assertTrue(tmpFile.exists());

          try (ZipFile zipFile = new ZipFile(tmpFile)) {
            List<ZipArchiveEntry> entries = Collections.list(zipFile.getEntries());

            entries.forEach(e -> Assert.assertFalse(e.getName().contains("exception.txt")));

            List<String> tables =
                IOUtils.readLines(
                    zipFile.getInputStream(zipFile.getEntry("tables.jsonl")),
                    StandardCharsets.UTF_8);

            Assert.assertEquals(
                "All tables should be present.", NUM_SCHEMAS * NUM_TABLES, tables.size());

            LOG.info("Dump verified.");
          }

        } catch (Exception e) {
          throw new AssertionError(e);
        } finally {
          if (!debug) {
            FileUtils.forceDelete(tmpFile);
          }
        }
      }
    }
  }

  private void assertNoException(Future<Void> f) {
    try {
      f.get();
    } catch (Exception e) {
      Assert.fail("Exception during setup");
    }
  }

  private Callable<Void> getCallable(
      HiveServerSupport support, List<String> batch, ConcurrentProgressMonitor monitor) {
    return () -> {
      support.execute(batch.toArray(new String[] {}));
      monitor.count(batch.size());
      return null;
    };
  }
}
