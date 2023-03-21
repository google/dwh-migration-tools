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
package com.google.edwmigration.dumper.common.io;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.common.util.ZipFileUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

/** @author shevek */
public class ZipArchiveEntryByteSource extends PathByteSource {

  @CheckForNull
  public static ZipArchiveEntryByteSource forZipEntryNameOrNull(
      @Nonnull String zipName, @Nonnull ZipFile zipFile, @Nonnull String zipEntryName) {
    Preconditions.checkNotNull(zipFile, "ZipFile was null.");
    Preconditions.checkNotNull(zipEntryName, "ZipEntryName was null.");
    ZipArchiveEntry zipEntry = ZipFileUtils.findZipEntry(zipFile, zipEntryName);
    if (zipEntry == null) return null;
    return new ZipArchiveEntryByteSource(zipName, zipFile, zipEntry);
  }

  @Nonnull
  public static ZipArchiveEntryByteSource forZipEntryName(
      @Nonnull String zipName, @Nonnull ZipFile zipFile, @Nonnull String zipEntryName)
      throws FileNotFoundException {
    ZipArchiveEntryByteSource source = forZipEntryNameOrNull(zipName, zipFile, zipEntryName);
    if (source == null) throw new FileNotFoundException("No " + zipEntryName + " in " + zipFile);
    return source;
  }

  private final String zipName;
  private final ZipFile zipFile;
  private final ZipArchiveEntry zipEntry;

  public ZipArchiveEntryByteSource(
      @Nonnull String zipName, @Nonnull ZipFile zipFile, @Nonnull ZipArchiveEntry zipEntry) {
    this.zipName = zipName;
    this.zipFile = zipFile;
    this.zipEntry = zipEntry;
  }

  @Nonnull
  @Override
  public String getPath() {
    return zipName + "!" + zipEntry;
  }

  @Nonnull
  public String getEntryName() {
    return zipEntry.getName();
  }

  @Override
  public Optional<Long> sizeIfKnown() {
    return Optional.of(zipEntry.getSize());
  }

  @Override
  public InputStream openStream() throws IOException {
    return zipFile.getInputStream(zipEntry);
  }

  @Override
  public String toString() {
    return "ZipEntryByteSource(" + getPath() + ")";
  }
}
