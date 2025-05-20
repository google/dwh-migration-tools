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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

import javax.annotation.Nonnull;

/**
 * All names in here are lowercase because they are the aliases from the redshift projection, and
 * the dumper did not quote them, so they were lowercased by the server because redshift is a
 * CASE_SMASH_LOWER dialect.
 */
public interface RedshiftLogsDumpFormat {

  String FORMAT_NAME = "redshift.logs.zip";

  public static final String ZIP_ENTRY_PREFIX_METRICS = "metrics_history_";
  public static final String ZIP_ENTRY_PREFIX_SCANS = "scan_history_";
  public static final String ZIP_ENTRY_SUFFIX = ".csv";

  public static enum DdlHistory {
    userid,
    starttime,
    endtime,
    label,
    xid,
    pid,
    sqltext;
    public static final String ZIP_ENTRY_PREFIX = "ddl_history_";

    public static boolean isZipEntryName(@Nonnull String name) {
      return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
    }
  }

  public static enum QueryHistory {
    queryid,
    xid,
    pid,
    userid,
    starttime,
    endtime,
    label,
    sqltext;

    public static final String ZIP_ENTRY_PREFIX = "query_history_";

    public static boolean isZipEntryName(@Nonnull String name) {
      return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
    }
  }

  /**
   * UNLOAD ('select * from svl_statementtext order by starttime, xid, sequence') TO $LOCATION
   * $CREDENTIALS delimiter as ',' addquotes escape allowoverwrite ;
   */
  public static interface SvlStatementText {

    public static enum Header {
      userid,
      xid,
      pid,
      label,
      starttime,
      endtime,
      sequence,
      type,
      text,
      qid;
    }
  }
}
