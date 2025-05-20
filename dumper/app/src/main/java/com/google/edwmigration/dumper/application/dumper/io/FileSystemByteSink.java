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
package com.google.edwmigration.dumper.application.dumper.io;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
@ThreadSafe
public class FileSystemByteSink extends ByteSink {

  private final Path path;
  private final OutputHandle.WriteMode writeMode;

  public FileSystemByteSink(@Nonnull Path path, @Nonnull OutputHandle.WriteMode writeMode) {
    this.path = Preconditions.checkNotNull(path, "Path was null.");
    this.writeMode = writeMode;
  }

  @Override
  public OutputStream openStream() throws IOException {
    switch (writeMode) {
      case CREATE_TRUNCATE:
        // Default options are CREATE, TRUNCATE_EXISTING, WRITE
        return Files.newOutputStream(path);
      case APPEND_EXISTING:
        // Implicitly includes WRITE
        return Files.newOutputStream(path, StandardOpenOption.APPEND);
      default:
        throw new UnsupportedOperationException("Unsupported write mode: " + writeMode);
    }
  }

  @Override
  public String toString() {
    return "FileSystemByteSink(" + path.getFileSystem() + "!" + path + ")";
  }
}
