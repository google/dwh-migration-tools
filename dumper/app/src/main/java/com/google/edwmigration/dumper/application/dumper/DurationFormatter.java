/*
 * Copyright 2022-2024 Google LLC
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

import com.google.common.collect.ImmutableList;
import java.time.Duration;

public class DurationFormatter {

  public static String formatApproximateDuration(Duration duration) {
    long minutes = duration.toMinutes() % 60;
    long hours = duration.toHours();

    if (hours == 0 && minutes == 0) {
      return "less than one minute";
    }

    ImmutableList.Builder<String> tokens = ImmutableList.builder();
    if (hours > 0) {
      tokens.add(Long.toString(hours));
      tokens.add(hours > 1 ? "hours" : "hour");
    }
    if (minutes > 0) {
      tokens.add(Long.toString(minutes));
      tokens.add(minutes > 1 ? "minutes" : "minute");
    }
    return "~" + String.join(" ", tokens.build());
  }
}
