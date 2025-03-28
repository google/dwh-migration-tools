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
package com.google.edwmigration.permissions.commands.expand;

import com.google.edwmigration.permissions.models.HdfsPermission;
import com.google.edwmigration.permissions.models.HdfsUser;
import java.util.function.Function;
import java.util.stream.Stream;

public class HdfsUserReader implements StreamProcessor<HdfsUser> {

  private CsvFileStreamProcessor<HdfsPermission> csvReader;

  public HdfsUserReader(String dumperHdfs) {
    csvReader =
        new CsvFileStreamProcessor<>(
            HdfsPermission.CSV_MAPPER, dumperHdfs, "hdfs.csv", HdfsPermission.class);
  }

  @Override
  public <R> R process(Function<Stream<HdfsUser>, R> operator) {
    return csvReader.process(
        hdfsPermissionStream ->
            operator.apply(
                hdfsPermissionStream.map(HdfsPermission::owner).distinct().map(HdfsUser::create)));
  }
}
