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
package com.google.edwmigration.permissions.commands.buildcommand;

import com.google.edwmigration.permissions.commands.CommandArgsHelp;
import java.io.IOException;
import javax.annotation.Nonnull;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class BuildOptions {

  public static final String OPT_DUMPER_RANGER = "ranger-dumper-output";

  public static final String OPT_DUMPER_HDFS = "hdfs-dumper-output";

  public static final String OPT_TABLES = "tables";

  public static final String OPT_PRINCIPALS = "principals";

  public static final String OPT_PERMISSIONS_RULESET = "permissions-ruleset";

  public static final String OPT_OUTPUT_PERMISSIONS = "output-permissions";

  private static final OptionParser parser = new OptionParser();

  private static final OptionSpec<String> optionDumperRanger =
      parser
          .accepts(
              OPT_DUMPER_RANGER,
              "Local or GCS path to the Ranger dumper output. It can be a ZIP file or unzipped directory.")
          .withRequiredArg()
          .describedAs("/path/to/ranger-dumper-output.zip")
          .required();

  private static final OptionSpec<String> optionDumperHdfs =
      parser
          .accepts(
              OPT_DUMPER_HDFS,
              "Local or GCS path to the HDFS dumper output. It can be a ZIP file or unzipped directory.")
          .withRequiredArg()
          .describedAs("/path/to/hdfs-dumper-output.zip");

  private static final OptionSpec<String> optionTables =
      parser
          .accepts(OPT_TABLES, "GCS path to tables metadata, should end with slash /.")
          .withRequiredArg()
          .describedAs("gs://BUCKET_NAME/tables/")
          .required();

  private static final OptionSpec<String> optionPrincipals =
      parser
          .accepts(OPT_PRINCIPALS, "Local or GCS path to the principals yaml file.")
          .withRequiredArg()
          .describedAs("/path/to/principals.yaml")
          .required();

  private static final OptionSpec<String> optionPermissionsConfig =
      parser
          .accepts(
              OPT_PERMISSIONS_RULESET, "Local or GCS path to the permissions config yaml file.")
          .withRequiredArg()
          .describedAs("/path/to/permissions-config.yaml")
          .required();

  private static final OptionSpec<String> optionOutputPermissions =
      parser
          .accepts(OPT_OUTPUT_PERMISSIONS, "Local or GCS path to the output permissions.yaml file.")
          .withRequiredArg()
          .defaultsTo("permissions.yaml")
          .describedAs("/path/to/permissions.yaml");

  private static final OptionSpec<Integer> optionNumThreads =
      parser
          .accepts("num-threads", "Number of parallel threads.")
          .withRequiredArg()
          .ofType(Integer.class)
          .defaultsTo(256);

  private static final OptionSpec<Integer> optionTimeoutSeconds =
      parser
          .accepts("timeout-seconds", "Timeout in seconds after which the command is interrupted.")
          .withRequiredArg()
          .ofType(Integer.class)
          .defaultsTo(24 * 60 * 60);

  private static final CommandArgsHelp help = new CommandArgsHelp(parser);

  private final OptionSet options;

  public BuildOptions(String[] args) {
    options = parser.parse(args);
  }

  public boolean handleHelp() throws IOException {
    return help.handle(options);
  }

  @Nonnull
  public String getDumperRanger() {
    return options.valueOf(optionDumperRanger);
  }

  public String getDumperHdfs() {
    return options.valueOf(optionDumperHdfs);
  }

  @Nonnull
  public String getTables() {
    return options.valueOf(optionTables);
  }

  @Nonnull
  public String getPrincipals() {
    return options.valueOf(optionPrincipals);
  }

  @Nonnull
  public String getPermissionsRuleset() {
    return options.valueOf(optionPermissionsConfig);
  }

  @Nonnull
  public String getOutputPermissions() {
    return options.valueOf(optionOutputPermissions);
  }

  public int getNumThreads() {
    return options.valueOf(optionNumThreads);
  }

  public int getTimeoutSeconds() {
    return options.valueOf(optionTimeoutSeconds);
  }
}
