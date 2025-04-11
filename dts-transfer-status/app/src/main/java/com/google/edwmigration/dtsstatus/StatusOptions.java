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
package com.google.edwmigration.dtsstatus;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class StatusOptions {

  private static final OptionParser parser = new OptionParser();
  private static final OptionSpec<Void> optionListTransferConfigs =
      parser.accepts("list-transfer-configs");
  private static final OptionSpec<Void> optionListStatusForConfig =
      parser.accepts("list-status-for-config");
  private static final OptionSpec<Void> optionListStatusForDatabase =
      parser.accepts("list-status-for-database");
  private static final OptionSpec<String> optionProjectId =
      parser.accepts("project-id").withRequiredArg().ofType(String.class);
  private static final OptionSpec<String> location =
      parser.accepts("location").withRequiredArg().ofType(String.class);
  private static final OptionSpec<String> optionConfigId =
      parser.accepts("config-id").withRequiredArg().ofType(String.class);
  private static final OptionSpec<String> optionDatabase =
      parser.accepts("database").withRequiredArg().ofType(String.class);

  private final OptionSet options;

  public StatusOptions(String[] args) {
    options = parser.parse(args);
  }

  public boolean hasListTransferConfigs() {
    return options.has(optionListTransferConfigs);
  }

  public boolean hasListStatusForConfig() {
    return options.has(optionListStatusForConfig);
  }

  public boolean hasListStatusForDatabase() {
    return options.has(optionListStatusForDatabase);
  }

  public boolean hasProjectId() {
    return options.has(optionProjectId);
  }

  public boolean hasLocation() {
    return options.has(location);
  }

  public boolean hasDatabase() {
    return options.has(optionDatabase);
  }

  public String getProjectId() {
    return options.valueOf(optionProjectId);
  }

  public String getLocation() {
    return options.valueOf(location);
  }

  public boolean hasConfigId() {
    return options.has(optionConfigId);
  }

  public String getConfigId() {
    return options.valueOf(optionConfigId);
  }

  public String getDatabase() {
    return options.valueOf(optionDatabase);
  }
}
