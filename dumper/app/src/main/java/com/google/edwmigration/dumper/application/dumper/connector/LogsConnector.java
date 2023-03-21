/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector;

import java.text.Format;
import java.text.SimpleDateFormat;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;

/** @author shevek */
public interface LogsConnector extends Connector {

  @Nonnull
  default String getDefaultFileName(boolean isAssessment) {
    Format format = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    String timeSuffix = "-" + format.format(System.currentTimeMillis());
    return String.format(
        "dwh-migration-%s-logs%s.zip", getName(), isAssessment ? timeSuffix : StringUtils.EMPTY);
  }
}
