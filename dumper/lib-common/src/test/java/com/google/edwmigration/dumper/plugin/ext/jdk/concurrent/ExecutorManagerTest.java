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

import static org.junit.Assert.assertEquals;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
@RunWith(JUnit4.class)
public class ExecutorManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorManagerTest.class);

  @SuppressWarnings("FutureReturnValueIgnored")
  private void test(@Nonnull ExecutorManager manager, int niter) throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();

    final int n = 5000;
    final AtomicInteger completed = new AtomicInteger();

    manager.await();
    for (int i = 0; i < niter; i++) {
      for (int j = 0; j < n; j++) {
        final String name = "i=" + i + ", j=" + j;
        final int tm = i % 1024;

        manager.submit(
            new Callable<Object>() {
              @Override
              public Void call() throws Exception {
                // LOG.info("Start " + name);
                Thread.sleep(tm);
                // LOG.info("End " + name);
                completed.getAndIncrement();
                return null;
              }
            });
        manager.submit(
            new Runnable() {
              @Override
              public void run() {
                try {
                  Thread.sleep(tm);
                  completed.getAndIncrement();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            },
            i);

        manager.execute(
            new Callable<Object>() {
              @Override
              public Void call() throws Exception {
                // LOG.info("Start " + name);
                Thread.sleep(tm);
                // LOG.info("End " + name);
                completed.getAndIncrement();
                return null;
              }
            });
        manager.execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  Thread.sleep(tm);
                  completed.getAndIncrement();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });
      }
      manager.await();
      assertEquals((i + 1) * n * 4, completed.get());
    }
    manager.await();
    assertEquals(niter * n * 4, completed.get());

    LOG.debug("Done in " + stopwatch);
  }

  @Test
  public void testParallel() throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    ExecutorManager manager = new ExecutorManager(executor);
    test(manager, 2);
    executor.shutdownNow();
  }

  @Test
  public void testParallelLimited() throws Exception {
    ExecutorService executor =
        new ThreadPoolExecutor(
            2,
            2,
            1,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10),
            new ThreadPoolExecutor.CallerRunsPolicy());
    ExecutorManager manager = new ExecutorManager(executor);
    test(manager, 2);
    executor.shutdownNow();
  }

  private void testThrow(@Nonnull ExecutorManager manager) throws Exception {
    manager.await();
    for (int i = 0; i < 10; i++) {
      final String name = "i=" + i;
      final int tm = i;
      manager.execute(
          new Callable<Object>() {
            @Override
            public Void call() throws Exception {
              LOG.info("Throw " + name);
              Thread.sleep(tm);
              throw new IllegalStateException("Throw " + name);
            }
          });
    }
    manager.await();
  }

  @Test(expected = ExecutionException.class)
  public void testSequentialThrow() throws Exception {
    ExecutorManager manager = new ExecutorManager(MoreExecutors.newDirectExecutorService());
    testThrow(manager);
  }

  @Test(expected = ExecutionException.class)
  public void testParallelThrow() throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    ExecutorManager manager = new ExecutorManager(executor);
    testThrow(manager);
    executor.shutdownNow();
  }
}
