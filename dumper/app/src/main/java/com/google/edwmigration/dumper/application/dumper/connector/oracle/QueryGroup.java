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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.auto.value.AutoValue;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

@AutoValue
@ParametersAreNonnullByDefault
abstract class QueryGroup {

  abstract boolean required();

  @Nonnull
  abstract StatsSource statsSource();

  @Nonnull
  abstract TenantSetup tenantSetup();

  @Nonnull
  static QueryGroup create(boolean required, StatsSource statsSource, TenantSetup tenantSetup) {
    return new AutoValue_QueryGroup(required, statsSource, tenantSetup);
  }

  @Nonnull
  String path() {
    return tenantSetup().code + "/" + statsSource().value;
  }

  /** The source of performance statistics. */
  enum StatsSource {
    AWR("awr"),
    NATIVE("native"),
    STATSPACK("statspack");

    final String value;

    StatsSource(String value) {
      this.value = value;
    }
  }

  enum TenantSetup {
    MULTI_TENANT("cdb"),
    SINGLE_TENANT("dba");

    final String code;

    TenantSetup(String code) {
      this.code = code;
    }
  }
}
