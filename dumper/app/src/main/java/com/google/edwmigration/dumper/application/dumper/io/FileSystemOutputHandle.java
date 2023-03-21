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
package com.google.edwmigration.dumper.application.dumper.io;

import com.google.common.io.ByteSink;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class FileSystemOutputHandle implements OutputHandle {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemOutputHandle.class);

  private final Path temporaryPath;
  private final Path targetPath;

  public FileSystemOutputHandle(@Nonnull Path rootPath, @Nonnull String targetPath) {
    // Due to the semantics of prepare(), both of these must be within the same subdirectory.
    this.targetPath = rootPath.resolve(targetPath);
    this.temporaryPath = rootPath.resolve(targetPath + ".tmp");
    // LOG.debug("Created " + this);
  }

  @Override
  public boolean exists() throws IOException {
    // LOG.debug("Looking for existence of " + targetPath);
    return Files.exists(targetPath);
  }

  @Override
  public ByteSink asByteSink() throws IOException {
    // LOG.debug("As ByteSink: " + this + " = " + targetPath);
    prepare();
    return new FileSystemByteSink(targetPath);
  }

  @Override
  public ByteSink asTemporaryByteSink() throws IOException {
    // LOG.debug("As Temporary ByteSink: " + this + " = " + temporaryPath);
    prepare();
    return new FileSystemByteSink(temporaryPath);
  }

  /**
   * Ensures that the target file can be written.
   *
   * <p>Must be called before calling openStream() on a ByteStream acquired from this object.
   */
  private void prepare() throws IOException {
    // Ensures that the directory to which we want to write exists
    Files.createDirectories(targetPath.getParent());
  }

  @Override
  public void commit() throws IOException {
    if (!Files.exists(temporaryPath))
      throw new FileNotFoundException(
          "File does not exist: " + temporaryPath + "; cannot move to " + targetPath);
    Files.move(temporaryPath, targetPath);
  }

  @Override
  public String toString() {
    return "FileSystemOutputHandle(" + targetPath.getFileSystem() + "!" + targetPath + ")";
  }
}
