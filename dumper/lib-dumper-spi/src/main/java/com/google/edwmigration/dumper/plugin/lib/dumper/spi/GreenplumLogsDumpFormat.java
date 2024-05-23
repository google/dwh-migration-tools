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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

import javax.annotation.Nonnull;

/**
 * All names in here are lowercase because they are the aliases from the redshift projection, and
 * the dumper did not quote them, so they were lowercased by the server because redshift is a
 * CASE_SMASH_LOWER dialect.
 */
public interface GreenplumLogsDumpFormat {

  public static final String FORMAT_NAME = "greenplum.logs.zip";
  public static final String ZIP_ENTRY_SUFFIX = ".csv";

  public static interface QueryHistory {

    public static final String ZIP_ENTRY_PREFIX = "queryhistory_";

    public static enum Header {
      userid,
      xid,
      pid,
      query,
      label,
      starttime,
      endtime,
      sequence,
      text;
    }

    public static boolean isZipEntryName(@Nonnull String name) {
      return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
    }
  }

  
}
