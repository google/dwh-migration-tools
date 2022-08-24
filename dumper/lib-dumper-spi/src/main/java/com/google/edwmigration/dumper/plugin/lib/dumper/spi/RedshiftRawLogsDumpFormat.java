/*
 * Copyright 2022 Google LLC
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
 * All names in here are lowercase because they are the aliases from the redshift projection,
 * and the dumper did not quote them, so they were lowercased by the server because redshift is a
 * CASE_SMASH_LOWER dialect.
 */
public interface RedshiftRawLogsDumpFormat {

    public static final String FORMAT_NAME = "redshift-raw.logs.zip";
    public static final String ZIP_ENTRY_SUFFIX = ".csv";

    public static interface DdlHistory {

        public static final String ZIP_ENTRY_PREFIX = "ddltext_";

        public static enum Header {
            userid, xid, pid, label, starttime, endtime, sequence, text;
        }

        public static boolean isZipEntryName(@Nonnull String name) {
            return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
        }
    }

    public static interface QueryHistory {

        public static final String ZIP_ENTRY_PREFIX = "querytext_";

        public static enum Header {
            userid, xid, pid, query, label, starttime, endtime, sequence, text;
        }

        public static boolean isZipEntryName(@Nonnull String name) {
            return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
        }
    }

    public static interface QueryMetricsHistory {

        public static final String ZIP_ENTRY_PREFIX = "querymetrics_";

        public static enum Header {
            userid, service_class, query, segment, step_type, starttime, slices, max_rows, rows,
            max_cpu_time, cpu_time, max_blocks_read, blocks_read, max_run_time, run_time,
            max_blocks_to_disk, blocks_to_disk, step, max_query_scan_size, query_scan_size,
            query_priority, query_queue_time, service_class_name;

        }

        public static boolean isZipEntryName(@Nonnull String name) {
            return name.startsWith(ZIP_ENTRY_PREFIX) && name.endsWith(ZIP_ENTRY_SUFFIX);
        }
    }
}
