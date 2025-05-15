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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;

public class MatchingIteratorTest {

  @Test
  public void next_emitsMatchingElements() {
    ImmutableList<Entry<Integer, Integer>> actual =
        MatchingIterator.mergeJoinStream(
                ImmutableList.of(1, 1, 1, 2, 4).iterator(),
                ImmutableList.of(2, 3, 4, 5).iterator(),
                Integer::compare)
            .collect(toImmutableList());

    ImmutableList<Entry<Integer, Integer>> expected =
        ImmutableList.of(new SimpleEntry<>(2, 2), new SimpleEntry<>(4, 4));
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void next_skipsDuplicates() {
    ImmutableList<Entry<Integer, Integer>> actual =
        MatchingIterator.mergeJoinStream(
                ImmutableList.of(1, 1, 1, 2, 2, 2, 4, 4, 4).iterator(),
                ImmutableList.of(2, 3, 4, 5).iterator(),
                Integer::compare)
            .collect(toImmutableList());

    ImmutableList<Entry<Integer, Integer>> expected =
        ImmutableList.of(new SimpleEntry<>(2, 2), new SimpleEntry<>(4, 4));
    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void next_withoutMatches() {
    ImmutableList<Entry<Integer, Integer>> actual =
        MatchingIterator.mergeJoinStream(
                ImmutableList.of(1, 1, 1, 4, 4, 4).iterator(),
                ImmutableList.of(2, 2, 3).iterator(),
                Integer::compare)
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(ImmutableList.of());
  }

  @Test
  public void next_emptyRhs() {
    ImmutableList<Entry<Integer, Integer>> actual =
        MatchingIterator.mergeJoinStream(
                ImmutableList.<Integer>of().iterator(),
                ImmutableList.of(1, 2, 3).iterator(),
                Integer::compare)
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(ImmutableList.of());
  }

  @Test
  public void next_emptyLhs() {
    ImmutableList<Entry<Integer, Integer>> actual =
        MatchingIterator.mergeJoinStream(
                ImmutableList.of(1, 2, 3).iterator(),
                ImmutableList.<Integer>of().iterator(),
                Integer::compare)
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(ImmutableList.of());
  }
}
