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
package com.google.edwmigration.permissions.models;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import org.junit.jupiter.api.Test;

public class HdfsPermissionTest {

  @Test
  public void create_parsesModificationTime() throws JsonMappingException, JsonProcessingException {
    CsvSchema schema =
        HdfsPermission.CSV_MAPPER
            .typedSchemaFor(HdfsPermission.class)
            .withHeader()
            .withColumnReordering(true);
    String header =
        "Path,FileType,FileSize,Owner,Group,Permission,ModificationTime,FileCount,DirCount,StoragePolicy\n";
    String stringHdfsPerm = header + "empty,,,,,,2024-10-02 00:00:00.000,,,";
    HdfsPermission hdfsPerm =
        HdfsPermission.CSV_MAPPER
            .readerFor(HdfsPermission.class)
            .with(schema)
            .readValue(stringHdfsPerm);

    LocalDateTime expectedModificationTime = LocalDate.of(2024, Month.OCTOBER, 2).atStartOfDay();
    assertThat(hdfsPerm.modificationTime()).isEqualTo(expectedModificationTime);
    assertThat(hdfsPerm.path()).isEqualTo("empty");
    assertThat(hdfsPerm.fileType()).isEmpty();
    assertThat(hdfsPerm.fileSize()).isNull();
  }
}
