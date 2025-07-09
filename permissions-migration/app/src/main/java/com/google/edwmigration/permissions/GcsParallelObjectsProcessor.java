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
package com.google.edwmigration.permissions;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsParallelObjectsProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(GcsParallelObjectsProcessor.class);
  private final GcsPath sourcePath;
  private final int numThreads;
  private final int timeoutSeconds;

  public GcsParallelObjectsProcessor(GcsPath sourcePath, int numThreads, int timeoutSeconds) {

    this.sourcePath = sourcePath.normalizePathSuffix();
    this.numThreads = numThreads;
    this.timeoutSeconds = timeoutSeconds;
  }

  public void Run(Consumer<Blob> consumer) {
    Storage storage = StorageOptions.getDefaultInstance().getService();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicInteger blobsCounter = new AtomicInteger(0);
    List<Future<?>> futures = new ArrayList<>();

    try {
      Bucket bucket = storage.get(sourcePath.bucketName());
      bucket
          .list(Storage.BlobListOption.prefix(sourcePath.objectName()))
          .iterateAll()
          .forEach(
              blob ->
                  futures.add(
                      executor.submit(
                          () -> {
                            consumer.accept(blob);
                            blobsCounter.incrementAndGet();
                          })));
    } catch (Exception e) {
      LOG.error("Error processing bucket", e);
    } finally {
      executor.shutdown();
      try {
        if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
      }
    }

    LOG.info("Processed: {} items.", blobsCounter.get());

    // If any action ended up with an exception, it will be wrapped in an ExecutionException.
    // Rethrow the first one if it occurred.
    for (Future<?> future : futures) {
      if (future.isDone()) {
        try {
          future.get();
        } catch (InterruptedException e) {
          throw new RuntimeException("Parallel processing of GCS objects has been interrupted");
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }
      }
    }
  }
}
