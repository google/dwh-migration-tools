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
package com.google.edwmigration.permissions.commands.buildcommand;

import com.google.common.collect.AbstractIterator;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.CheckForNull;

/**
 * An iterator that emits matching entries of two ordered collections. Each element can be matched
 * at most once, duplicates - if present - won't be emitted.
 */
public class MatchingIterator<T, U> extends AbstractIterator<Entry<T, U>> {

  private final Iterator<T> lhsIter;

  private final Iterator<U> rhsIter;

  private final BiFunction<T, U, Integer> comparator;

  public static <T, U> Stream<Entry<T, U>> mergeJoinStream(
      Iterator<T> lhsIter, Iterator<U> rhsIter, BiFunction<T, U, Integer> comparator) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            new MatchingIterator<>(lhsIter, rhsIter, comparator), Spliterator.ORDERED),
        false);
  }

  public MatchingIterator(
      Iterator<T> lhsIter, Iterator<U> rhsIter, BiFunction<T, U, Integer> comparator) {
    this.lhsIter = lhsIter;
    this.rhsIter = rhsIter;
    this.comparator = comparator;
  }

  @CheckForNull
  @Override
  protected Entry<T, U> computeNext() {
    if (!lhsIter.hasNext() || !rhsIter.hasNext()) {
      return endOfData();
    }
    T left = lhsIter.next();
    U right = rhsIter.next();
    for (; ; ) {
      int result = comparator.apply(left, right);
      if (result == 0) {
        // Emit a match.
        return new SimpleEntry<>(left, right);
      }
      if (result < 0) {
        if (!lhsIter.hasNext()) {
          return endOfData();
        }
        left = lhsIter.next();
      } else {
        if (!rhsIter.hasNext()) {
          return endOfData();
        }
        right = rhsIter.next();
      }
    }
  }
}
