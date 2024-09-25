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
package com.google.edwmigration.dumper.application.dumper.utils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import javax.annotation.Nonnull;

public class QueryLogDateUtil {

  @Nonnull private static ZonedDateTime queryLogStartDate = ZonedDateTime.now();

  @Nonnull
  private static ZonedDateTime queryLogEndDate =
      ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));

  public static ZonedDateTime getActualQueryLogStartDate() {
    return queryLogStartDate;
  }

  public static ZonedDateTime getActualQueryLogEndDate() {
    return queryLogEndDate;
  }

  public static void updateQueryLogDates(
      ZonedDateTime logQueryStarDate, ZonedDateTime logQueryEndDate) {
    QueryLogDateUtil.updateQueryLogStartDate(logQueryStarDate);
    QueryLogDateUtil.updateQueryLogEndDate(logQueryEndDate);
  }

  private static void updateQueryLogStartDate(ZonedDateTime newQueryLogStartDate) {
    if (newQueryLogStartDate == null) {
      return;
    }
    if (newQueryLogStartDate.isBefore(queryLogStartDate)) {
      queryLogStartDate = newQueryLogStartDate;
    }
  }

  private static void updateQueryLogEndDate(ZonedDateTime newQueryLogEndDate) {
    if (newQueryLogEndDate == null) {
      return;
    }
    if (newQueryLogEndDate.isAfter(queryLogEndDate)) {
      queryLogEndDate = newQueryLogEndDate;
    }
  }
}
