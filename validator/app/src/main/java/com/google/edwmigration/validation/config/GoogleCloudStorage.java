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
package com.google.edwmigration.validation.config;

import com.google.edwmigration.validation.deformed.ValidationProperty;
import com.google.edwmigration.validation.deformed.ValidationSchema;
import java.util.List;
import java.util.Map;

public class GoogleCloudStorage {
  public String gcsPath;
  public String projectId;

  public static final ValidationSchema<GoogleCloudStorage> buildSchema() {
    return new ValidationSchema<GoogleCloudStorage>(
        Map.of(
            "gcsPath",
            List.of(
                ValidationProperty.requiredString("gcsPath is required", s -> s.gcsPath),
                ValidationProperty.of(
                    "gcsPath is malformed. Expected pattern: gs://<bucket>[/optional/path]",
                    s -> {
                      return (s.gcsPath != null)
                          ? s.gcsPath.matches("^gs://[a-z0-9._-]+(/.*)?$")
                          : false;
                    })),
            "projectId",
            List.of(ValidationProperty.requiredString("projectId is required", s -> s.projectId))));
  }
}
