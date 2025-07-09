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

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.edwmigration.permissions.GcsPath;
import com.google.edwmigration.permissions.ProcessingException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsPathResolver implements PathResolver {
  private static final Logger LOG = LoggerFactory.getLogger(GcsPathResolver.class);

  @Override
  public boolean canSupport(String filePath) {
    return GcsPath.isValid(filePath);
  }

  @Override
  public <T> T apply(String filePath, Function<Path, T> process) {
    GcsPath gcsPath = GcsPath.parse(filePath);
    LOG.info("Creating path using Cloud Storage FileSystem: '{}'", gcsPath);
    try (FileSystem fs = CloudStorageFileSystem.forBucket(gcsPath.bucketName())) {
      Path gcsFilePath = fs.getPath(gcsPath.objectName());
      return process.apply(gcsFilePath);
    } catch (IOException e) {
      throw new ProcessingException(
          String.format("Error occurred when processing GCS file: '%s'", gcsPath), e);
    }
  }
}
