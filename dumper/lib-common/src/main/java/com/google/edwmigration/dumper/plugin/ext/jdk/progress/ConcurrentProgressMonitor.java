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
package com.google.edwmigration.dumper.plugin.ext.jdk.progress;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * A thread-safe progress monitor.
 *
 * <p>All methods on this class except close() may safely be called with arbitrary concurrency.
 *
 * @author shevek
 */
public interface ConcurrentProgressMonitor extends AutoCloseable {

  /** The divisor for memory measurements: 1 Mb. */
  public static final int MEMDIV = 1024 * 1024;

  @Nonnull
  public default ConcurrentProgressMonitor withBlockSize(@Nonnegative int blockSize) {
    return this;
  }

  /** Returns the current count. This may be a relatively slow operation. */
  @Nonnegative
  public long getCount();

  public void count(@Nonnegative int delta);

  /** Counts 1. */
  public default void count() {
    count(1);
  }

  @Override
  public void close();
}
