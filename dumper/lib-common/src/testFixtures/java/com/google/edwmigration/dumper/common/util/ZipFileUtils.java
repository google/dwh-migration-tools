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
package com.google.edwmigration.dumper.common.util;

import com.google.common.base.Preconditions;
import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class ZipFileUtils {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(ZipFileUtils.class);

  @CheckForNull
  public static ZipArchiveEntry findZipEntry(@Nonnull ZipFile zip, @Nonnull String name) {
    Preconditions.checkNotNull(zip, "ZipFile was null.");
    Preconditions.checkNotNull(name, "ZipEntryName was null.");

    ROOT:
    {
      ZipArchiveEntry entry = zip.getEntry(name);
      if (entry != null) return entry;
    }

    // Once in a while, somebody messes with a zip file, and really fouls it up.
    String suffix = "/" + name;
    for (ZipArchiveEntry entry : Collections.list(zip.getEntries())) {
      if (StringUtils.endsWithIgnoreCase(entry.getName(), suffix)) {
        LOG.warn(
            "SUSPICIOUS ZIP FILE: ALL BETS ARE OFF: Found zip entry "
                + name
                + " located at "
                + entry.getName()
                + ", not at the root.");
        return entry;
      }
    }

    return null;
  }
}
