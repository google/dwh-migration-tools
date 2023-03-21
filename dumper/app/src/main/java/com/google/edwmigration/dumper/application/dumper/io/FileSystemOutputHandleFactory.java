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
import java.nio.file.FileSystem;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

/** @author shevek */
@ThreadSafe
public class FileSystemOutputHandleFactory implements OutputHandleFactory {

  private final Path rootPath;

  public FileSystemOutputHandleFactory(@Nonnull Path rootPath) {
    this.rootPath = Preconditions.checkNotNull(rootPath, "Root path was null.");
  }

  public FileSystemOutputHandleFactory(@Nonnull FileSystem fileSystem, @Nonnull String rootPath) {
    this(fileSystem.getPath(rootPath));
  }

  @Nonnull
  @Override
  public OutputHandle newOutputFileHandle(@Nonnull String targetPath) {
    return new FileSystemOutputHandle(rootPath, targetPath);
  }

  @Override
  public String toString() {
    return "FileSystemOutputHandleFactory(" + rootPath.getFileSystem() + "!" + rootPath + ")";
  }
}
