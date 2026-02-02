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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera.manager.model;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class SparkYarnApplicationMetadata {

  public static SparkYarnApplicationMetadata create(
      String clusterName, String applicationId, String sparkVersion, String sparkApplicationType) {
    return new AutoValue_SparkYarnApplicationMetadata(
        clusterName, applicationId, sparkVersion, sparkApplicationType);
  }

  public abstract String getClusterName();

  public abstract String getApplicationId();

  public abstract String getSparkVersion();

  public abstract String getSparkApplicationType();
}
