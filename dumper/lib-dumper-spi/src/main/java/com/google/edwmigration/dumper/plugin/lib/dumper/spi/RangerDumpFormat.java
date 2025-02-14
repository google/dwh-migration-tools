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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public interface RangerDumpFormat {

  String FORMAT_NAME = "ranger.dump.zip";

  ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
          .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
          .disable(SerializationFeature.INDENT_OUTPUT);

  interface ServicesFormat {

    String ZIP_ENTRY_NAME = "services.jsonl";
  }

  interface PoliciesFormat {

    String ZIP_ENTRY_NAME = "policies.jsonl";
  }

  interface UsersFormat {

    String ZIP_ENTRY_NAME = "users.jsonl";
  }

  interface GroupsFormat {

    String ZIP_ENTRY_NAME = "groups.jsonl";
  }

  interface RolesFormat {

    String ZIP_ENTRY_NAME = "roles.jsonl";
  }
}
