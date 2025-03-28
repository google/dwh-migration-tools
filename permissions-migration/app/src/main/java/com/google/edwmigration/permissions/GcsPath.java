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
package com.google.edwmigration.permissions;

import com.google.auto.value.AutoValue;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

@AutoValue
public abstract class GcsPath {
  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("^gs://([^/]+)/(.*)$");

  public static GcsPath create(String bucketName, String objectName) {
    return new AutoValue_GcsPath(bucketName, objectName);
  }

  public static boolean isValid(String path) {
    return GCS_PATH_PATTERN.matcher(path).matches();
  }

  public static GcsPath parse(String path) {
    Matcher matcher = GCS_PATH_PATTERN.matcher(path);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Invalid GCS path format: " + path);
    }
    String bucketName = matcher.group(1);
    String objectName = matcher.group(2);
    return create(bucketName, objectName);
  }

  public abstract String bucketName();

  public abstract String objectName();

  /** Returns a GcsPath which ends with a slash "/" */
  public GcsPath normalizePathSuffix() {
    if (objectName().endsWith("/")) {
      return this;
    }
    return create(bucketName(), objectName() + "/");
  }

  @Override
  public String toString() {
    return "gs://" + bucketName() + "/" + objectName();
  }
}
