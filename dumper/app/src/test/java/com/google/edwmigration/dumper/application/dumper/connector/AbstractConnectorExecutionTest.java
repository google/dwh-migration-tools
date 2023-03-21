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
package com.google.edwmigration.dumper.application.dumper.connector;

import static org.junit.Assert.*;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.io.ByteSource;
import com.google.edwmigration.dumper.application.dumper.MetadataDumper;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.common.io.ZipArchiveEntryByteSource;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.CoreMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.MetadataDumperConstants;
import com.google.edwmigration.dumper.test.SystemPropertyValue;
import com.google.edwmigration.dumper.test.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author matt */
public abstract class AbstractConnectorExecutionTest extends AbstractConnectorTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectorExecutionTest.class);

  protected static final String SUBPROJECT = "compilerworks-application-dumper";

  private static final Path JDBC_DRIVERS =
      new File(TestUtils.getTestResourcesDir(SUBPROJECT), "jdbc-drivers/").toPath();

  // -Dtest.dumper to run dumper agains real databases. Off by default for jenkins.
  private static class IsDumperTest {

    public static final String NAME = "test.dumper";
    private static final boolean VALUE = SystemPropertyValue.get(NAME);
  }

  public static boolean isDumperTest() {
    if (IsDumperTest.VALUE)
      LOG.debug(
          "Running dumpers against database, -Dtest-sys-prop.{}=false to disable",
          IsDumperTest.NAME);
    else
      LOG.debug(
          "NOT running dumpers against database, -Dtest-sys-prop.{}=true to enable",
          IsDumperTest.NAME);
    return IsDumperTest.VALUE;
  }

  public boolean run(@Nonnull String... args) throws Exception {
    if (!isDumperTest()) {
      final int N = args.length;
      String[] dryRunArgs = Arrays.copyOf(args, N + 1);
      dryRunArgs[N] = "--dry-run";
      runDumper(dryRunArgs);
      return false;
    } else {
      runDumper(args);
      return true;
    }
  }

  // TODO: take multiple baseName to load multiple jar
  @CheckForNull
  protected static List<String> findJdbcDrivers(@Nonnull String baseName) throws IOException {
    // LOG.debug(baseName + " IN " + JDBC_DRIVERS.toString());
    return Files.find(
            JDBC_DRIVERS,
            1,
            (path, attr) -> {
              String name = path.getFileName().toString();
              return name.startsWith(baseName) && name.endsWith(".jar");
            })
        .map(p -> p.toAbsolutePath().toString())
        .collect(Collectors.toList());
  }

  public void runDumper(@Nonnull String... args) throws Exception {
    MetadataDumper dumper = new MetadataDumper();
    dumper.run(args);
  }

  protected static class ZipEntryValidator<E extends Enum<E>> {

    private final String entryName;
    private final Class<E> headerEnumClass;
    private E[] headerRequired = null;
    private boolean headerStrict = false;
    private Consumer<CSVRecord> recordConsumer =
        new Consumer<CSVRecord>() {
          private boolean seen = false;

          @Override
          public void accept(CSVRecord t) {
            if (seen) return;
            seen = true;
            LOG.debug(entryName + ": Sample record: " + t);
          }
        };
    private int recordCountMin = 1;
    private int recordCountMax = Integer.MAX_VALUE;

    public ZipEntryValidator(String entryName, Class<E> headerEnumClass) {
      this.entryName = entryName;
      this.headerEnumClass = headerEnumClass;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public final ZipEntryValidator<E> withHeaderRequired(E... headerRequired) {
      this.headerRequired = headerRequired;
      return this;
    }

    public ZipEntryValidator<E> withHeaderStrict() {
      this.headerStrict = true;
      return this;
    }

    public ZipEntryValidator<E> withRecordConsumer(Consumer<CSVRecord> recordConsumer) {
      this.recordConsumer = recordConsumer;
      return this;
    }

    public ZipEntryValidator<E> withRecordCount(int min, int max) {
      this.recordCountMin = min;
      this.recordCountMax = max;
      return this;
    }

    public ZipEntryValidator<E> withRecordCountIgnored() {
      return withRecordCount(0, Integer.MAX_VALUE);
    }

    public void run(@Nonnull ByteSource zipEntryByteSource) throws Exception {
      try (BufferedReader reader =
          zipEntryByteSource.asCharSource(StandardCharsets.UTF_8).openBufferedStream()) {
        CSVParser parser = new CSVParser(reader, AbstractTask.FORMAT.withFirstRecordAsHeader());

        Map<String, Integer> headerMap = parser.getHeaderMap();
        LOG.debug(zipEntryByteSource + ": CSV header map: {}", headerMap);

        if (headerStrict) {
          // Assert that all fields in CSV are valid header fields.
          for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            E field = Enum.valueOf(headerEnumClass, entry.getKey());
            assertEquals(field.ordinal(), (int) entry.getValue());
          }
        }

        // Assert that all valid header fields are in the CSV.
        if (headerRequired != null) {
          for (E headerItem : headerRequired)
            assertTrue(
                "Header map contains " + headerItem, headerMap.containsKey(headerItem.name()));
        }

        // Ensure presence of at least one record.
        int numRecords = 0;
        for (CSVRecord record : parser) {
          // Ensure each record maps a value to each header field (with no extraneous record
          // fields).
          assertEquals(headerMap.size(), record.size());

          if (headerRequired != null) {
            for (E headerItem : headerRequired) assertTrue(record.isMapped(headerItem.name()));
          }

          numRecords++;
          if (recordConsumer != null) recordConsumer.accept(record);
        }
        LOG.debug(zipEntryByteSource + ": Number of records: {}", numRecords);
        if (numRecords < recordCountMin || numRecords > recordCountMax)
          fail(
              "Record count out of range: "
                  + numRecords
                  + " not in ["
                  + recordCountMin
                  + ", "
                  + recordCountMax
                  + "]");
      }
    }
  }

  protected static class ZipValidator {

    private final Set<String> expectedEntries = new HashSet<>();
    private final Set<String> allowedEntries = new HashSet<>();
    @Nonnull private Predicate<? super String> allowedEntriesPredicate = Predicates.alwaysFalse();
    private final List<ZipEntryValidator<?>> entryValidators = new ArrayList<>();
    private String format;

    public ZipValidator() {
      expectedEntries.add(MetadataDumperConstants.VERSION_ZIP_ENTRY_NAME);
      expectedEntries.add(MetadataDumperConstants.ARGUMENTS_ZIP_ENTRY_NAME);
      expectedEntries.add(MetadataDumperConstants.FORMAT_ZIP_ENTRY_NAME);
      expectedEntries.add(
          CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.ZIP_ENTRY_NAME);
    }

    @Nonnull
    public ZipValidator withFormat(@Nonnull String format) {
      this.format = format;
      return this;
    }

    /** Specify names of files that are required to be in the zip file. */
    @Nonnull
    public ZipValidator withExpectedEntries(@Nonnull String... expectedEntries) {
      Collections.addAll(this.expectedEntries, expectedEntries);
      return this;
    }

    /**
     * Specify names of files that are allowed to be in the zip file, but whose absence is not an
     * error.
     *
     * <p>This predicate is considered additionally and separately to the explicitly allowed names.
     */
    @Nonnull
    public ZipValidator withAllowedEntries(@Nonnull Predicate<? super String> allowedEntries) {
      this.allowedEntriesPredicate =
          MoreObjects.firstNonNull(allowedEntries, Predicates.alwaysFalse());
      return this;
    }

    /**
     * Specify names of files that are allowed to be in the zip file, but whose absence is not an
     * error.
     *
     * <p>These explicitly allowed names are considered additionally and separately to the
     * predicate. The set of names is additive.
     */
    @Nonnull
    public ZipValidator withAllowedEntries(@Nonnull String... allowedEntries) {
      Collections.addAll(this.allowedEntries, allowedEntries);
      return this;
    }

    @Nonnull
    public <E extends Enum<E>> ZipEntryValidator<E> withEntryValidator(
        @Nonnull String entryName, @Nonnull Class<E> headerEnumClass) {
      withAllowedEntries(entryName);
      ZipEntryValidator<E> entryValidator = new ZipEntryValidator<>(entryName, headerEnumClass);
      entryValidators.add(entryValidator);
      return entryValidator;
    }

    public void run(@Nonnull ZipFile zipFile, @Nonnull String zipName) throws Exception {
      // Assert that all entries in zip are valid entries.
      for (ZipArchiveEntry entry : Collections.list(zipFile.getEntries())) {
        // if (!expectedEntries.contains(entry.getName()))
        // LOG.warn("Unexpected entry " + entry.getName());
        assertTrue(
            "Unexpected entry " + entry.getName(),
            expectedEntries.contains(entry.getName())
                || allowedEntries.contains(entry.getName())
                || allowedEntriesPredicate.test(entry.getName()));
      }

      // Assert that all valid entries are in the zip.
      for (String expectedEntry : expectedEntries) {
        ZipArchiveEntry zipEntry = zipFile.getEntry(expectedEntry);
        assertNotNull("Missing required entry " + expectedEntry, zipEntry);
      }

      FORMAT:
      {
        YAML:
        {
          ZipArchiveEntryByteSource source =
              ZipArchiveEntryByteSource.forZipEntryNameOrNull(
                  zipName,
                  zipFile,
                  CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.ZIP_ENTRY_NAME);
          if (source == null) break YAML;
          try (Reader reader = source.asCharSource(StandardCharsets.UTF_8).openStream()) {
            CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.Root root =
                CoreMetadataDumpFormat.MAPPER.readValue(
                    reader, CoreMetadataDumpFormat.CompilerWorksDumpMetadataTaskFormat.Root.class);
            String format = root.format;
            LOG.debug("Format is " + format);
            if (this.format != null) assertEquals("Format was wrong.", this.format, format);
          }
          break FORMAT;
        }
        TEXT:
        {
          ZipArchiveEntryByteSource source =
              ZipArchiveEntryByteSource.forZipEntryNameOrNull(
                  zipName, zipFile, MetadataDumperConstants.FORMAT_ZIP_ENTRY_NAME);
          if (source == null) break TEXT;
          String format = source.asCharSource(StandardCharsets.UTF_8).readFirstLine();
          LOG.debug("Format is " + format);
          if (this.format != null) assertEquals("Format was wrong.", this.format, format);
          break FORMAT;
        }
      }

      for (ZipEntryValidator<?> entryValidator : entryValidators) {
        entryValidator.run(
            ZipArchiveEntryByteSource.forZipEntryName(zipName, zipFile, entryValidator.entryName));
      }
    }

    public void run(@Nonnull File zipFile) throws Exception {
      run(new ZipFile(zipFile), zipFile.getPath());
    }
  }
}
