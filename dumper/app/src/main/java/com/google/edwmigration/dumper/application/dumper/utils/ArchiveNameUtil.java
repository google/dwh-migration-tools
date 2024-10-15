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

import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ArchiveNameUtil {

  public static final String ZIP_ENTRY_SUFFIX = ".csv";

  /**
   * Generate the archive file name that includes creation time. Typically this is used with a logs
   * connector when dumping for Assessment.
   *
   * @param name The name of the connector.
   * @param suffix Connector-specific suffix such as "metadata" or "logs".
   * @param clock The Clock instance to provide the date.
   * @return Full name for the .zip archive.
   */
  public static String getFileNameWithTimestamp(String name, Clock clock) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
    String timeSuffix = formatter.withZone(clock.getZone()).format(clock.instant());
    return formatFileName(name, timeSuffix);
  }

  public static String getEntryFileNameWithTimestamp(String prefix, ZonedInterval interval) {
    return getEntryFileNameWithTimestamp(prefix, interval, ZIP_ENTRY_SUFFIX);
  }

  public static String getEntryFileNameWithTimestamp(
      String prefix, ZonedInterval interval, String suffix) {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    return String.join("", prefix, formatter.format(interval.getStartUTC()), suffix);
  }

  /**
   * Generate the archive file name, but do not include creation time. This is the most common case,
   * dumping logs for assessment being a known exception.
   *
   * @param name The name of the connector.
   * @param suffix Connector-specific suffix such as "metadata" or "logs".
   * @return Full name for the .zip archive.
   */
  public static String getFileName(String name) {
    return formatFileName(name);
  }

  private static String formatFileName(String... terms) {
    return String.format("dwh-migration-%s.zip", String.join("-", terms));
  }

  private ArchiveNameUtil() {}
}
