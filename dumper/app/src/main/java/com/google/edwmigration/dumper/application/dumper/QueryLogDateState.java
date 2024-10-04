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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import javax.annotation.Nonnull;

public class QueryLogDateState {

  @Nonnull private static ZonedDateTime queryLogStartDate = ZonedDateTime.now();

  @Nonnull
  private static ZonedDateTime queryLogEndDate =
      ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

  public static synchronized ZonedDateTime getQueryLogFirstEntry() {
    return queryLogStartDate;
  }

  public static synchronized ZonedDateTime getQueryLogLastEntry() {
    return queryLogEndDate;
  }

  public static synchronized void updateQueryLogFirstEntry(ZonedDateTime newQueryLogStartDate) {
    if (newQueryLogStartDate == null) {
      return;
    }
    if (newQueryLogStartDate.isBefore(queryLogStartDate)) {
      queryLogStartDate = newQueryLogStartDate;
    }
  }

  public static synchronized void updateQueryLogLastEntry(ZonedDateTime newQueryLogEndDate) {
    if (newQueryLogEndDate == null) {
      return;
    }
    if (newQueryLogEndDate.isAfter(queryLogEndDate)) {
      queryLogEndDate = newQueryLogEndDate;
    }
  }
}
