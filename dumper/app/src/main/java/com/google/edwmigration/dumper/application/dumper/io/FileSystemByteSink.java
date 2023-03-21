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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
@ThreadSafe
public class FileSystemByteSink extends ByteSink {

  private final Path path;

  public FileSystemByteSink(@Nonnull Path path) {
    this.path = Preconditions.checkNotNull(path, "Path was null.");
  }

  @Override
  public OutputStream openStream() throws IOException {
    return Files.newOutputStream(path);
  }

  @Override
  public String toString() {
    return "FileSystemByteSink(" + path.getFileSystem() + "!" + path + ")";
  }
}
