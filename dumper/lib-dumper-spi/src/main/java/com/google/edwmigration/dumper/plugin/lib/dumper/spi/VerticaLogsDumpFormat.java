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

public interface VerticaLogsDumpFormat {

    String FORMAT_NAME = "vertica.logs.zip";

    String ZIP_ENTRY_PREFIX = "query_history_";

    /**
     * Lowercase, because Vertica is a CASE_SMASH_LOWER dialect and these are
     * not aliased by the Vertica logs dumper.
     */
    enum Header {
        time,
        node_name,
        session_id,
        user_id,
        user_name,
        transaction_id,
        statement_id,
        request_id,
        request_type,
        label,
        client_label,
        search_path,
        query_start_epoch,
        request,            // contains query text
        is_retry
    }
}
