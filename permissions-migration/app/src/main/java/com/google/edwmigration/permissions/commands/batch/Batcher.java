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
package com.google.edwmigration.permissions.commands.batch;

import com.google.cloud.storage.Blob;
import com.google.edwmigration.permissions.GcsParallelObjectsProcessor;
import com.google.edwmigration.permissions.GcsPath;
import com.google.re2j.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Batcher {
  private static final Logger LOG = LoggerFactory.getLogger(Batcher.class);
  private final GcsParallelObjectsProcessor gcsParallelObjectsProcessor;
  private final GcsPath sourcePath;
  private final GcsPath targetPath;
  private final Pattern pattern;

  public Batcher(
      GcsPath sourcePath, GcsPath targetPath, String pattern, int numThreads, int timeoutSeconds) {

    this.sourcePath = sourcePath.normalizePathSuffix();
    this.targetPath = targetPath.normalizePathSuffix();
    this.pattern = Pattern.compile(pattern);

    gcsParallelObjectsProcessor =
        new GcsParallelObjectsProcessor(sourcePath, numThreads, timeoutSeconds);
  }

  public void Run() {
    gcsParallelObjectsProcessor.Run(this::copyBlobIfMatchesPattern);
  }

  private void copyBlobIfMatchesPattern(Blob blob) {
    try {
      String blobName = blob.getName();
      LOG.info("Processing file {}", blobName);

      if (pattern.matcher(blobName).matches()) {
        String targetBlobName = getTargetName(blobName);
        blob.copyTo(targetPath.bucketName(), targetBlobName);
        LOG.info("File {} matched and copied to {}", blobName, targetBlobName);
      }
    } catch (Exception e) {
      LOG.error("Error processing blob {}", blob.getName(), e);
    }
  }

  private String getTargetName(String blobName) {
    // Get blob path relative to the source path.
    int prefixLength = sourcePath.objectName().length();
    String blobSuffix = blobName.substring(prefixLength);

    // Concatenate the target path (guaranteed to end in "/") with the relative
    // path.
    return targetPath.objectName() + blobSuffix;
  }
}
