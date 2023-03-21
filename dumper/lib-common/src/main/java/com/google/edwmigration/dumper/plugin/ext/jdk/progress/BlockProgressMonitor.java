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

/**
 * Calls underlying ProgressMonitor in blocks because calling Stopwatch is really expensive.
 *
 * @author shevek
 */
public class BlockProgressMonitor extends AbstractProgressMonitor {

  private final ProgressMonitor monitor;
  private long monitorCount;
  private final int blockSize;
  private int blockCount;

  public BlockProgressMonitor(@Nonnull ProgressMonitor monitor, @Nonnegative int blockSize) {
    this.monitor = monitor;
    this.blockSize = blockSize;
  }

  private void commit() {
    monitorCount = monitor.count(blockCount);
    blockCount = 0;
  }

  @Override
  public BlockProgressMonitor withBlockSize(@Nonnegative int blockSize) {
    commit();
    return monitor.withBlockSize(blockSize);
  }

  @Override
  public long timeElapsed(TimeUnit desiredUnit) {
    return monitor.timeElapsed(desiredUnit);
  }

  @Override
  public long getCount() {
    return monitor.getCount() + monitorCount;
  }

  @Override
  public long count(int delta) {
    blockCount += delta;
    if (blockCount >= blockSize) commit();
    return monitorCount + blockCount;
  }

  @Override
  public void close() {
    if (blockCount > 0) commit();
    monitor.close();
  }
}
