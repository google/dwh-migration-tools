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

import java.text.SimpleDateFormat;
import java.time.Clock;

public class ArchiveNameUtil {

  /**
   * Generate the archive file name that includes creation time. Typically this is used with a logs
   * connector when dumping for Assessment.
   *
   * @param nameWithOptionalSuffix The name of the connector, can be followed by a suffix such as "-metadata" or "-logs".
   * @param clock The Clock instance to provide the date.
   * @return Full name for the .zip archive.
   */
  public static String getFileNameWithTimestamp(String nameWithOptionalSuffix, Clock clock) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    String timeSuffix = format.format(clock.millis());
    return formatFileName(nameWithOptionalSuffix + timeSuffix);
  }

  /**
   * Generate the archive file name, but do not include creation time. This is the most common case,
   * dumping logs for assessment being a known exception.
   *
   * @param nameWithOptionalSuffix The name of the connector, can be followed by a suffix such as "-metadata" or "-logs"
   * @return Full name for the .zip archive.
   */
  public static String getFileName(String nameWithOptionalSuffix) {
    return formatFileName(nameWithOptionalSuffix);
  }

  private static String formatFileName(String nameWithOptionalSuffix) {
    return String.format("dwh-migration-%s.zip", nameWithOptionalSuffix);
  }

  private ArchiveNameUtil() {}
}
