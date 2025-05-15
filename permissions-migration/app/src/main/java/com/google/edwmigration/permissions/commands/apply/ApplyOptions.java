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
package com.google.edwmigration.permissions.commands.apply;

import com.google.edwmigration.permissions.ExtraPermissions;
import com.google.edwmigration.permissions.commands.CommandArgsHelp;
import java.io.IOException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ApplyOptions {
  private static final OptionParser parser = new OptionParser();

  private final OptionSet options;

  private static final OptionSpec<String> optionPermissions =
      parser
          .accepts(
              "permissions",
              "Local or GCS path to the input YAML file containing permission definitions.")
          .withRequiredArg()
          .describedAs("permissions.yaml")
          .required();

  private static final OptionSpec<ExtraPermissions> optionExtraPermissions =
      parser
          .accepts(
              "extra-permissions",
              "Behavior for pre-exising permissions: keep|purge. Currently only keep is supported.")
          .withRequiredArg()
          .ofType(ExtraPermissions.class)
          .defaultsTo(ExtraPermissions.KEEP);

  private static final CommandArgsHelp help = new CommandArgsHelp(parser);

  public ApplyOptions(String[] args) {
    options = parser.parse(args);
  }

  public boolean handleHelp() throws IOException {
    return help.handle(options);
  }

  public String getPermissionsFilename() {
    return options.valueOf(optionPermissions);
  }

  public ExtraPermissions getExtraPermissions() {
    return options.valueOf(optionExtraPermissions);
  }
}
