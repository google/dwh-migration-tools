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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalPathResolver implements PathResolver {
  private static final Logger LOG = LoggerFactory.getLogger(LocalPathResolver.class);
  private static final String FILE_SCHEME = "file";

  @Override
  public boolean canSupport(String filePath) {
    return Paths.get(filePath).toUri().getScheme().equals(FILE_SCHEME);
  }

  @Override
  public <T> T apply(String filePath, Function<Path, T> process) {
    LOG.info("Creating path on local file system: '{}'", filePath);
    return process.apply(Paths.get(filePath));
  }
}
