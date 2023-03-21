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

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class ConcurrentRecordProgressMonitor extends AbstractConcurrentProgressMonitor {

  private static class ExecutorHolder {

    private static final ThreadFactory THREAD_FACTORY =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("progressmonitor-%d")
            .setUncaughtExceptionHandler(
                new Thread.UncaughtExceptionHandler() {
                  @Override
                  public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Uncaught exception in " + t.getName() + ": " + e, e);
                  }
                })
            .build();
    private static final ScheduledExecutorService EXECUTOR_SERVICE =
        MoreExecutors.getExitingScheduledExecutorService(
            new ScheduledThreadPoolExecutor(1, THREAD_FACTORY));
  }

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentRecordProgressMonitor.class);

  private static final long DELAY = 6;
  private final String name;
  private final long total;
  private final LongAdder count = new LongAdder();
  private final Stopwatch stopwatch = Stopwatch.createStarted();
  private final ScheduledFuture<?> future;

  public ConcurrentRecordProgressMonitor(@Nonnull String name, @Nonnegative long total) {
    this.name = name;
    this.total = total;
    LOG.debug(name + ": Starting with " + total + " items. " + newMemorySummary());
    this.future =
        ExecutorHolder.EXECUTOR_SERVICE.scheduleWithFixedDelay(
            new Update(this, name), DELAY, DELAY, TimeUnit.SECONDS);
  }

  public ConcurrentRecordProgressMonitor(@Nonnull String name) {
    this.name = name;
    this.total = 0;
    LOG.debug(name + ": Starting. " + newMemorySummary());
    this.future =
        ExecutorHolder.EXECUTOR_SERVICE.scheduleWithFixedDelay(
            new Update(this, name), DELAY, DELAY, TimeUnit.SECONDS);
  }

  @Nonnegative
  public long getTotal() {
    return total;
  }

  @Override
  public long getCount() {
    return count.longValue();
  }

  private void update(@Nonnull String status) {
    // A bit fiddly, but permits both the comparison with 0 and the double divide.
    long count = getCount();
    long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    String rate =
        (elapsed == 0) ? "infinity" : Long.toString((long) (count * 1000L / (double) elapsed));
    if (total > 0) {
      LOG.debug(
          name
              + ": "
              + status
              + " "
              + count
              + " / "
              + total
              + " items ("
              + String.format("%2.2f", 100d * count / total)
              + "%) in "
              + stopwatch
              + " ("
              + rate
              + "/sec) "
              + newMemorySummary());
    } else {
      LOG.debug(
          name
              + ": "
              + status
              + " "
              + count
              + " items in "
              + stopwatch
              + " ("
              + rate
              + "/sec) "
              + newMemorySummary());
    }
  }

  private static class Update implements Runnable {

    private final WeakReference<ConcurrentRecordProgressMonitor> reference;
    private final String name;

    public Update(
        @UnderInitialization ConcurrentRecordProgressMonitor monitor, @Nonnull String name) {
      this.reference = new WeakReference<>(monitor);
      // this.name = monitor.name;    // This isn't provably non-null with UnderInitialization.
      this.name = name;
    }

    @Override
    public void run() {
      ConcurrentRecordProgressMonitor monitor = reference.get();
      if (monitor == null)
        throw new IllegalStateException(
            "Monitor " + name + " garbage-collected without being closed.");
      monitor.update("Processed");
    }
  }

  @Override
  public void count(int delta) {
    count.add(delta);
  }

  @Override
  public void close() {
    if (!future.isDone()) {
      future.cancel(false);
      update("Completed");
      // LOG.debug(name + ": Done in " + stopwatch + ".");
    }
  }
}
