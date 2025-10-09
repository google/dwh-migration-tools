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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.time.Duration;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class DurationFormatterTest {

  @DataPoints("durations")
  public static final ImmutableList<Pair<Duration, String>> DURATIONS =
      ImmutableList.of(
          Pair.of(Duration.ZERO, "less than 15 minutes"),
          Pair.of(Duration.ofMillis(1), "less than 15 minutes"),
          Pair.of(Duration.ofSeconds(1), "less than 15 minutes"),
          Pair.of(Duration.ofMinutes(2), "less than 15 minutes"),
          Pair.of(Duration.ofMinutes(2).plusSeconds(1), "less than 15 minutes"),
          Pair.of(Duration.ofMinutes(14).plusSeconds(59), "less than 15 minutes"),
          Pair.of(Duration.ofMinutes(15), "~15 minutes"),
          Pair.of(Duration.ofMinutes(21), "~21 minutes"),
          Pair.of(Duration.ofMinutes(34), "~34 minutes"),
          Pair.of(Duration.ofMinutes(59).plusSeconds(59).plusMillis(999), "~59 minutes"),
          Pair.of(Duration.ofHours(1), "~1 hour"),
          Pair.of(Duration.ofHours(1).plusSeconds(1), "~1 hour"),
          Pair.of(Duration.ofHours(1).plusMinutes(1), "~1 hour 1 minute"),
          Pair.of(Duration.ofHours(1).plusMinutes(7), "~1 hour 7 minutes"),
          Pair.of(Duration.ofHours(1).plusMinutes(7).plusSeconds(12), "~1 hour 7 minutes"),
          Pair.of(Duration.ofHours(1).plusMinutes(59), "~1 hour 59 minutes"),
          Pair.of(Duration.ofHours(2), "~2 hours"),
          Pair.of(Duration.ofHours(2).plusMinutes(1), "~2 hours 1 minute"),
          Pair.of(Duration.ofHours(10), "~10 hours"),
          Pair.of(Duration.ofHours(10).plusMinutes(1), "~10 hours 1 minute"),
          Pair.of(Duration.ofHours(11), "~11 hours"),
          Pair.of(Duration.ofHours(21), "~21 hours"),
          Pair.of(Duration.ofHours(24), "~24 hours"),
          Pair.of(Duration.ofHours(300), "~300 hours"),
          Pair.of(Duration.ofHours(300).plusMinutes(44), "~300 hours 44 minutes"),
          Pair.of(Duration.ofHours(300).plusMinutes(44).plusMillis(1), "~300 hours 44 minutes"),
          Pair.of(Duration.ofHours(10000), "~10000 hours"),
          Pair.of(Duration.ofHours(1000000), "~1000000 hours"));

  @Theory
  public void format_success(@FromDataPoints("durations") Pair<Duration, String> durationPair) {
    Duration duration = durationPair.getLeft();
    String expectedFormattedDuration = durationPair.getRight();

    String actualFormattedDuration = DurationFormatter.formatApproximateDuration(duration);

    assertEquals(expectedFormattedDuration, actualFormattedDuration);
  }
}
