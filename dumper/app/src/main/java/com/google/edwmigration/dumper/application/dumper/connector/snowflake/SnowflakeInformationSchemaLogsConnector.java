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
package com.google.edwmigration.dumper.application.dumper.connector.snowflake;

import com.google.auto.service.AutoService;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;

/** @author shevek */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Snowflake, using INFORMATION_SCHEMA only.")
public class SnowflakeInformationSchemaLogsConnector extends SnowflakeLogsConnector {

  public SnowflakeInformationSchemaLogsConnector() {
    super("snowflake-information-schema-logs");
  }
}
