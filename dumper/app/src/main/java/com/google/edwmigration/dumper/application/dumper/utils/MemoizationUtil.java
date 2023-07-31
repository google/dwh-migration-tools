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
package com.google.edwmigration.dumper.application.dumper.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.function.IOFunction;

/** Utility class for memoization. */
public class MemoizationUtil {

  /**
   * Creates a memoizer that stores results in a map using the key created by the key function. When
   * called with a value v, the map is checked if there already is an entry for keyFunction(v). If
   * so, the corresponding value is returned. Otherwise, the delegate is called and the result is
   * stored.
   */
  public static <T, R> IOFunction<T, R> createMemoizer(IOFunction<T, R> delegate) {
    LoadingCache<T, R> cache =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<T, R>() {
                  public R load(T t) throws IOException {
                    return delegate.apply(t);
                  }
                });
    return t -> {
      try {
        return cache.get(t);
      } catch (UncheckedExecutionException e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          throw new RuntimeException("Got unexpected UncheckedExecutionException.", e);
        }
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new RuntimeException("Got unexpected ExecutionException.", e);
        }
      }
    };
  }

  private MemoizationUtil() {}
}
