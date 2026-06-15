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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.io.OutputHandle.WriteMode;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask.TaskOptions;
import java.io.IOException;
import java.net.URL;

enum FeaturesQueryPath {
  SIMPLE("account-usage-simple.sql") {
    @Override
    TaskOptions taskOptions() {
      return TaskOptions.DEFAULT.withWriteMode(WriteMode.APPEND_EXISTING);
    }
  },
  COMPLEX("account-usage-complex.sql") {
    @Override
    TaskOptions taskOptions() {
      return TaskOptions.DEFAULT;
    }
  },
  SHOW_BASED("show-based.sql") {
    @Override
    TaskOptions taskOptions() {
      return TaskOptions.DEFAULT;
    }
  };

  private final String file;

  FeaturesQueryPath(String file) {
    this.file = file;
  }

  abstract TaskOptions taskOptions();

  String loadFile() {
    String value = "snowflake-features/" + file;
    try {
      URL queryUrl = getResource(value);
      return Resources.toString(queryUrl, UTF_8);
    } catch (IOException e) {
      String message = String.format("An invalid file was provided: '%s'.", value);
      throw new IllegalArgumentException(message, e);
    }
  }
}
