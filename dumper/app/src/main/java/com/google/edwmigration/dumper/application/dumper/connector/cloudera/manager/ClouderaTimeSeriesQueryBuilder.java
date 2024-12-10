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

import com.google.common.base.Preconditions;
import javax.annotation.Nonnull;

public abstract class ClouderaTimeSeriesQueryBuilder {
  protected int includedLastDays;
  protected TimeSeriesAggregation tsAggregation;

  public ClouderaTimeSeriesQueryBuilder(
      int includedLastDays, @Nonnull TimeSeriesAggregation tsAggregation) {
    Preconditions.checkNotNull(tsAggregation, "TimeSeriesAggregation has not to be a null.");
    Preconditions.checkArgument(
        includedLastDays >= 1,
        "The chart has to include at least one day. Received " + includedLastDays + " days.");
    this.includedLastDays = includedLastDays;
    this.tsAggregation = tsAggregation;
  }

  abstract String getQuery(String entityName);

  public int getIncludedLastDays() {
    return includedLastDays;
  }

  public TimeSeriesAggregation getTsAggregation() {
    return tsAggregation;
  }

  enum TimeSeriesAggregation {
    RAW,
    TEN_MINUTELY,
    HOURLY,
    SIX_HOURLY,
    DAILY,
    WEEKLY,
  }
}
