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

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/** @author shevek */
public interface ProgressMonitor extends AutoCloseable {

  /** The divisor for memory measurements: 1 Mb. */
  public static final int MEMDIV = 1024 * 1024;

  @Nonnull
  public default BlockProgressMonitor withBlockSize(@Nonnegative int blockSize) {
    return new BlockProgressMonitor(this, blockSize);
  }

  /** Returns the time elapsed since creation of this ProgressMonitor */
  public long timeElapsed(TimeUnit desiredUnit);

  /** Returns the current count. */
  @Nonnegative
  public default long getCount() {
    return count(0);
  }

  /** Adds delta to the current count. Returns the number counted so far, including this count. */
  public long count(@Nonnegative int delta);

  /** Adds 1 to the current count. Returns the number counted so far, including this count. */
  public default long count() {
    return count(1);
  }

  @Override
  public void close();
}
