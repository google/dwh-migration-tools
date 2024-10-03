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

import java.time.ZonedDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QueryLogSharedState {
  public static final ConcurrentMap<QueryLogEntry, ZonedDateTime> queryLogEntries =
      new ConcurrentHashMap<>();

  public enum QueryLogEntry {
    QUERY_LOG_FIRST_ENTRY,
    QUERY_LOG_LAST_ENTRY
  }

  /*
   * Calculates first and last query log entries, by applying 'min' and 'max' logic.
   */
  public static void updateQueryLogEntries(QueryLogEntry logEntry, ZonedDateTime newDateTime) {
    ZonedDateTime currentDateTime = QueryLogSharedState.queryLogEntries.get(logEntry);
    if (currentDateTime == null) {
      QueryLogSharedState.queryLogEntries.put(logEntry, newDateTime);
    } else {
      if (logEntry == QueryLogEntry.QUERY_LOG_FIRST_ENTRY && newDateTime.isBefore(currentDateTime)
          || logEntry == QueryLogEntry.QUERY_LOG_LAST_ENTRY
              && newDateTime.isAfter(currentDateTime)) {
        QueryLogSharedState.queryLogEntries.put(logEntry, newDateTime);
      }
    }
  }
}
