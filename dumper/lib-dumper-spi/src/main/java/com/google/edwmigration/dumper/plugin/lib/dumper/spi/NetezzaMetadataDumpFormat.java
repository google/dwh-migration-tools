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

/**
 *
 * @author shevek
 */
public interface NetezzaMetadataDumpFormat {

    public static final String FORMAT_NAME = "netezza.dump.zip";

    public interface Views {

        public static final String ZIP_ENTRY_SUFFIX = "/nz.v_view.csv";

        // Not sure of a docref for this.
        public static enum Header {
            DATABASE,
            SCHEMA,
            VIEWNAME,
            DEFINITION
        }
    }

    public interface DistMapFormat {

        public static final String ZIP_ENTRY_SUFFIX = "/nz.v_table_dist_map.csv";

        public static enum DistMapHeader {
            OBJID,
            TABLENAME,
            OWNER,
            CREATEDATE,
            DISTSEQNO, // 1-based index
            DISTATTNUM,
            ATTNUM,
            ATTNAME,
            DATABASE,
            OBJDB,
            SCHEMA,
            SCHEMAID
        }
    }

}
