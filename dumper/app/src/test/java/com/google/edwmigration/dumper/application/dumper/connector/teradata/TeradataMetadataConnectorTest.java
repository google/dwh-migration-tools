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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask.setParameterValues;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.ResourceLocation;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorExecutionTest;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.ProgressMonitor;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataMetadataDumpFormat;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.UrlResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.util.FileSystemUtils;

/**
 * ./gradlew :compilerworks-application-dumper:test --tests
 * TeradataMetadataConnectorTest.testConnectorSynthetic -Dtest.verbose=true
 * -Dtest-sys-prop.test.dumper=true
 */
@RunWith(JUnit4.class)
public class TeradataMetadataConnectorTest extends AbstractConnectorExecutionTest
    implements TeradataMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(TeradataMetadataConnectorTest.class);

  private final TeradataMetadataConnector connector = new TeradataMetadataConnector();

  private static class HugeDatabase {
    private static final int N_DB = 50;
    private static final int N_TBL = 1000;
    private static final int N_COL = 200;
  }

  private static class SmallDatabase {
    private static final int N_DB = 2;
    private static final int N_TBL = 3;
    private static final int N_COL = 5;
  }

  @Test
  public void testConnector() throws Exception {
    testConnectorDefaults(connector);
  }

  private static boolean exec(@Nonnull PreparedStatement statement, Object... arguments)
      throws SQLException {
    setParameterValues(statement, arguments);
    return statement.execute();
  }

  private enum TableKind {
    /** Permanent and base temporary tables */
    T,
    /** Join index. */
    I,
    /** Hash index. */
    N,
    /** NOPI tables. */
    O,
    /** Queue tables. */
    Q,
    /** View stats. */
    V,
  }

  @Nonnull
  private byte[] newTableId(int i_db, int i_tbl) {
    byte i_db1 = (byte) (i_db >> 8);
    byte i_db0 = (byte) i_db;

    byte i_tbl3 = (byte) (i_tbl >> 24);
    byte i_tbl2 = (byte) (i_tbl >> 16);
    byte i_tbl1 = (byte) (i_tbl >> 8);
    byte i_tbl0 = (byte) i_tbl;

    return new byte[] {i_db1, i_db0, i_tbl3, i_tbl2, i_tbl1, i_tbl0};
  }

  @Test
  public void testConnectorSynthetic() throws Exception {
    Assume.assumeTrue(isDumperTest());

    File outputFile =
        runDumper(
            "teradata-simulation-schema.sql",
            HugeDatabase.N_DB,
            HugeDatabase.N_TBL,
            HugeDatabase.N_COL);

    ZipValidator validator =
        new ZipValidator()
            .withFormat(FORMAT_NAME)
            .withExpectedEntries(
                VersionFormat.ZIP_ENTRY_NAME,
                // ColumnsFormat.ZIP_ENTRY_NAME, // We're not dumping this anymore
                DatabasesVFormat.ZIP_ENTRY_NAME,
                TablesVFormat.ZIP_ENTRY_NAME,
                TableTextVFormat.ZIP_ENTRY_NAME,
                ColumnsVFormat.ZIP_ENTRY_NAME,
                IndicesVFormat.ZIP_ENTRY_NAME,
                ColumnsQVFormat.ZIP_ENTRY_NAME,
                ColumnsJQVFormat.ZIP_ENTRY_NAME,
                FunctionsVFormat.ZIP_ENTRY_NAME,
                StatsVFormat.ZIP_ENTRY_NAME,
                TableSizeVFormat.ZIP_ENTRY_NAME);
    validator.withEntryValidator(DatabasesVFormat.ZIP_ENTRY_NAME, DatabasesVFormat.Header.class);
    validator.withEntryValidator(TablesVFormat.ZIP_ENTRY_NAME, TablesVFormat.Header.class);
    validator.withEntryValidator(ColumnsVFormat.ZIP_ENTRY_NAME, ColumnsVFormat.Header.class);
    validator
        .withEntryValidator(TableSizeVFormat.ZIP_ENTRY_NAME, TableSizeVFormat.Header.class)
        .withRecordCountIgnored();
    validator.run(outputFile);
  }

  @Test
  public void testConnectorSyntheticSmall() throws Exception {
    Assume.assumeTrue(isDumperTest());

    File outputFile =
        runDumper(
            "teradata-simulation-schema.sql",
            SmallDatabase.N_DB,
            SmallDatabase.N_TBL,
            SmallDatabase.N_COL);

    ZipValidator validator =
        new ZipValidator()
            .withFormat(FORMAT_NAME)
            .withExpectedEntries(
                VersionFormat.ZIP_ENTRY_NAME,
                // ColumnsFormat.ZIP_ENTRY_NAME, // We're not dumping this anymore
                DatabasesVFormat.ZIP_ENTRY_NAME,
                TablesVFormat.ZIP_ENTRY_NAME,
                TableTextVFormat.ZIP_ENTRY_NAME,
                ColumnsVFormat.ZIP_ENTRY_NAME,
                IndicesVFormat.ZIP_ENTRY_NAME,
                ColumnsQVFormat.ZIP_ENTRY_NAME,
                ColumnsJQVFormat.ZIP_ENTRY_NAME,
                FunctionsVFormat.ZIP_ENTRY_NAME,
                StatsVFormat.ZIP_ENTRY_NAME,
                TableSizeVFormat.ZIP_ENTRY_NAME);
    validator.withEntryValidator(DatabasesVFormat.ZIP_ENTRY_NAME, DatabasesVFormat.Header.class);
    validator.withEntryValidator(TablesVFormat.ZIP_ENTRY_NAME, TablesVFormat.Header.class);
    validator.withEntryValidator(ColumnsVFormat.ZIP_ENTRY_NAME, ColumnsVFormat.Header.class);
    validator
        .withEntryValidator(TableSizeVFormat.ZIP_ENTRY_NAME, TableSizeVFormat.Header.class)
        .withRecordCountIgnored();
    validator.run(outputFile);
  }

  @Nullable
  private File runDumper(String schemaSql, final int nDb, final int nTbl, final int nCol)
      throws Exception {
    if (true) {
      DataSource dataSource = new DriverManagerDataSource("jdbc:postgresql:cw", "cw", "password");
      LOG.debug("Resetting database...");
      try (Connection connection = dataSource.getConnection()) {
        URL url = Resources.getResource(ResourceLocation.class, schemaSql);
        ScriptUtils.executeSqlScript(connection, new UrlResource(url));
      }
      JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

      LOG.debug("Rebuilding database....");
      try (ProgressMonitor monitor =
          new RecordProgressMonitor("Creating sample schema", nDb * nTbl * nCol)
              .withBlockSize(256)) {
        jdbcTemplate.execute(
            new ConnectionCallback<Void>() {
              @Override
              public Void doInConnection(Connection connection)
                  throws SQLException, DataAccessException {
                PreparedStatement q_db =
                    connection.prepareStatement(
                        "insert into DBC.DBase (\"DatabaseName\", \"DatabaseId\") values (?, ?)");
                PreparedStatement q_tbl =
                    connection.prepareStatement(
                        "insert into DBC.TVM (\"DatabaseId\", \"TVMNameI\", \"TVMName\", \"TVMId\", \"TableKind\") values (?, ?, ?, ?, ?)");
                PreparedStatement q_col =
                    connection.prepareStatement(
                        "insert into DBC.TVFields (\"TableId\", \"FieldName\", \"FieldId\", \"FieldType\") values (?, ?, ?, ?)");

                connection.setAutoCommit(false);
                for (int i_db = 0; i_db < nDb; i_db++) {
                  byte[] databaseId = Ints.toByteArray(0xFF010000 | i_db);
                  String databaseName = "db_" + i_db;
                  exec(q_db, databaseName, databaseId);
                  for (int i_tbl = 0; i_tbl < nTbl; i_tbl++) {
                    byte[] tableId = newTableId(i_db, i_tbl);
                    String tableName = "tbl_" + i_db + "_" + i_tbl;
                    exec(q_tbl, databaseId, tableName, tableName, tableId, TableKind.T.name());
                    q_col.clearBatch();
                    for (int i_col = 0; i_col < nCol; i_col++) {
                      short columnId = (short) i_col;
                      String columnName = "col_" + i_db + "_" + i_tbl + "_" + i_col;
                      setParameterValues(q_col, tableId, columnName, columnId, "CV");
                      q_col.addBatch();
                      monitor.count();
                    }
                    q_col.executeBatch();
                    q_col.clearBatch();
                  }
                }
                connection.commit();

                return null;
              }
            });
      }
    }

    File outputFile = TestUtils.newOutputFile("compilerworks-teradata-metadata-synthetic.zip");
    FileSystemUtils.deleteRecursively(outputFile);

    List<String> args =
        Arrays.asList(
            "--connector",
            connector.getName(),
            "--output",
            outputFile.getAbsolutePath(),
            "--continue",
            "--jdbcDriver",
            org.postgresql.Driver.class.getName(),
            "--url",
            "jdbc:postgresql:cw",
            "--user",
            "cw",
            "--password",
            "password"
            // "--database", "db_0"
            );

    MetadataDumper dumper = new MetadataDumper().withExitOnError(false);

    dumper.run(args.toArray(ArrayUtils.EMPTY_STRING_ARRAY));

    CONTINUE:
    {
      // Prove that --continue is doing its thing.
      Stopwatch stopwatch = Stopwatch.createStarted();
      dumper.run(args.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
      assertTrue(
          "Second run of dumper was too slow.",
          stopwatch.elapsed(TimeUnit.SECONDS)
              < 10); // The main dump takes about 2 minutes. This noop should take 1 second.
    }
    return outputFile;
  }
}
