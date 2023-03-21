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
package com.google.edwmigration.dumper.plugin.ext.jdk.concurrent;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class ExecutorManager implements AutoCloseable, Executor {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorManager.class);
  private static final boolean DEBUG = false;

  @Nonnull
  public static ExecutorService newExecutorServiceWithBackpressure(
      @Nonnull String name, int nthreads, int qfactor) {
    if (nthreads <= 1) return MoreExecutors.newDirectExecutorService();
    // The constant 40 here is related to the asymmetry of tasks: How many tasks
    // must be in the queue to keep the executor threads busy if the main thread
    // decides to go out to lunch with a CallerRunsPolicy.
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            nthreads,
            nthreads,
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(nthreads * qfactor),
            new ThreadPoolExecutor.CallerRunsPolicy());
    executor.setThreadFactory(
        new ThreadFactoryBuilder().setNameFormat(name + "-%d").setDaemon(true).build());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  @Nonnull
  public static ExecutorService newExecutorServiceWithBackpressure(
      @Nonnull String name, int nthreads) {
    return newExecutorServiceWithBackpressure(name, nthreads, 40);
  }

  @Nonnull
  public static ExecutorService newExecutorServiceWithBackpressure(@Nonnull String name) {
    // We use ncpus - 1 because the backpressure feeds back into the source thread, which will
    // therefore run at 100% CPU as well.
    // So we can only have N-1 active worker threads.
    int nthreads = Runtime.getRuntime().availableProcessors() - 1;
    return newExecutorServiceWithBackpressure(name, nthreads);
  }

  @Nonnull
  public static ExecutorService newUnboundedExecutorService(@Nonnull String name, int nthreads) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            nthreads,
            nthreads,
            30L,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    executor.setThreadFactory(
        new ThreadFactoryBuilder().setNameFormat(name + "-%d").setDaemon(true).build());
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  @Nonnull
  public static ExecutorService newUnboundedExecutorService(@Nonnull String name) {
    int nthreads = Runtime.getRuntime().availableProcessors();
    return newUnboundedExecutorService(name, nthreads);
  }

  private class QueueingFuture<V> extends FutureTask<V> {

    public QueueingFuture(@Nonnull Callable<@Nullable V> callable) {
      super(callable);
    }

    public QueueingFuture(@Nonnull Runnable runnable, @CheckForNull V result) {
      super(runnable, result);
    }

    @Override
    protected void done() {
      completionQueue.add(this);
      if (DEBUG) {
        try {
          get();
        } catch (ExecutionException | InterruptedException e) {
          LOG.warn("Finished with exception: " + this);
        }
      }
    }
  }

  private static final int EXCEPTIONS_MAX = 100;
  private final Executor executor;
  private final BlockingQueue<Future<?>> completionQueue = new LinkedBlockingQueue<>();
  private final AtomicInteger outstanding = new AtomicInteger(0);

  @GuardedBy("exceptions")
  private final List<Exception> exceptions = new ArrayList<>(EXCEPTIONS_MAX);

  public ExecutorManager(@Nonnull Executor executor) {
    this.executor = executor;
  }

  private void get(@Nonnull Future<?> f) {
    try {
      Object v = f.get();
      if (DEBUG) LOG.debug(f + " -> " + v);
    } catch (InterruptedException | ExecutionException e) {
      if (DEBUG) LOG.debug(f + " -> " + e, e);
      synchronized (exceptions) {
        if (exceptions.size() < EXCEPTIONS_MAX) exceptions.add(e);
      }
    }
  }

  private void reap() {
    for (; ; ) {
      Future<?> f = completionQueue.poll();
      if (f == null) break;
      int n = outstanding.getAndDecrement();
      if (DEBUG) if (n < 0) LOG.debug("reap at " + n);
      get(f);
    }
  }

  @Nonnull
  private <V, T extends QueueingFuture<V>> T submit(@Nonnull T future) {
    reap();
    int n = outstanding.getAndIncrement();
    if (DEBUG) if (n < 0) LOG.debug("submit at " + n);
    executor.execute(future);
    return future;
  }

  @Nonnull
  public <V> QueueingFuture<V> submit(@Nonnull Callable<@Nullable V> r) {
    return submit(new QueueingFuture<>(r));
  }

  @Nonnull
  public <V> QueueingFuture<V> submit(@Nonnull Runnable r, @CheckForNull V result) {
    return submit(new QueueingFuture<>(r, result));
  }

  /** Like submit, but does not return the future, avoiding a warning. */
  @SuppressWarnings("FutureReturnValueIgnored")
  public <V> void execute(@Nonnull Callable<@Nullable V> r) {
    submit(new QueueingFuture<>(r));
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void execute(@Nonnull Runnable r) {
    submit(new QueueingFuture<>(r, null));
  }

  private static final IntUnaryOperator DECREMENT_IF_AVAILABLE =
      new IntUnaryOperator() {
        @Override
        public int applyAsInt(int operand) {
          return Math.max(operand - 1, 0);
        }
      };

  public void await() throws InterruptedException, ExecutionException {
    if (DEBUG) LOG.debug("Awaiting " + outstanding + " executions.");
    while (outstanding.getAndUpdate(DECREMENT_IF_AVAILABLE) > 0) {
      Future<?> f = completionQueue.take();
      get(f);
    }
    if (DEBUG) LOG.debug("Await done.");
    synchronized (exceptions) {
      if (exceptions.isEmpty()) return;
      Exception e = exceptions.get(0);
      for (int i = 1; i < exceptions.size(); i++) e.addSuppressed(exceptions.get(i));
      exceptions.clear();

      Throwables.propagateIfPossible(e, InterruptedException.class, ExecutionException.class);
      throw new ExecutionException(e);
    }
  }

  @Override
  public void close() throws InterruptedException, ExecutionException {
    // LOG.debug("Closing ExecutorManager", new Exception());
    await();
    // LOG.debug("Closed ExecutorManager", new Exception());
  }

  @Override
  public String toString() {
    return "ExecutorManager(" + executor + "; outstanding=" + outstanding + ")";
  }
}
