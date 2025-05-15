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
package com.google.edwmigration.permissions.files;

import com.google.edwmigration.permissions.ProcessingException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Wraps existing PathResolver to allow automatically reading files inside zip archives.
 * Files that are not zip archives are handled without changes.
 */
public class ZipPathResolverDecorator implements PathResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ZipPathResolverDecorator.class);
  private static final String ZIP_SUFFIX = ".zip";

  private final PathResolver innerResolver;

  public ZipPathResolverDecorator(PathResolver innerResolver) {
    this.innerResolver = innerResolver;
  }

  @Override
  public boolean canSupport(String filePath) {
    return innerResolver.canSupport(filePath);
  }

  @Override
  public <T> T apply(String filePath, Function<Path, T> process) {
    return innerResolver.apply(filePath, processWithZipFileSystem(process));
  }

  private static <T> Function<Path, T> processWithZipFileSystem(Function<Path, T> process) {
    return path -> {
      if (!path.toString().toLowerCase().endsWith(ZIP_SUFFIX)) {
        return process.apply(path);
      }
      LOG.trace("Creating path for ZIP file system: '{}'", path);
      try (FileSystem fs = FileSystems.newFileSystem(path, (ClassLoader) null)) {
        Path rootDir = fs.getPath("/");
        return process.apply(rootDir);
      } catch (IOException e) {
        throw new ProcessingException(
            String.format("Error occurred when processing zip file: %s", path), e);
      }
    };
  }
}
