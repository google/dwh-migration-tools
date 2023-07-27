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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.function.IOFunction;

/** Utility class for memoization. */
public class MemoizationUtil {

  private static class Memoizer<T, R> implements IOFunction<T, R> {

    private final IOFunction<T, R> delegate;
    private final Map<T, R> cache = new HashMap<>();

    public Memoizer(IOFunction<T, R> delegate) {
      this.delegate = delegate;
    }

    @Override
    public R apply(T t) throws IOException {
      if (cache.containsKey(t)) {
        return cache.get(t);
      }
      R result = delegate.apply(t);
      cache.put(t, result);
      return result;
    }
  }

  /**
   * Creates a memoizer that stores results in a map using the key created by the key function. When
   * called with a value v, the map is checked if there already is an entry for keyFunction(v). If
   * so, the corresponding value is returned. Otherwise, the delegate is called and the result is
   * stored.
   */
  public static <T, R> IOFunction<T, R> createMemoizer(IOFunction<T, R> delegate) {
    return new Memoizer<>(delegate);
  }

  private MemoizationUtil() {}
}
