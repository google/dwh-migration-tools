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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager;

import javax.annotation.Nonnull;

public class ClouderaRamTimeSeriesQueryBuilder extends ClouderaTimeSeriesQueryBuilder {

  private static final String TS_CPU_QUERY_TEMPLATE =
      "select swap_used, physical_memory_used, physical_memory_total, physical_memory_cached, physical_memory_buffers where entityName=\"%s\"";

  public ClouderaRamTimeSeriesQueryBuilder(
      int includedLastDays, @Nonnull TimeSeriesAggregation tsAggregation) {
    super(includedLastDays, tsAggregation);
  }

  @Override
  String getQuery(String hostId) {
    return String.format(TS_CPU_QUERY_TEMPLATE, hostId);
  }
}
