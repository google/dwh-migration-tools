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
package com.google.edwmigration.permissions.commands.expand;

import com.google.edwmigration.permissions.commands.CommandArgsHelp;
import java.io.IOException;
import javax.annotation.Nonnull;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ExpandOptions {

  public static final String OPT_DUMPER_RANGER = "ranger-dumper-output";

  public static final String OPT_DUMPER_HDFS = "hdfs-dumper-output";

  public static final String OPT_PRINCIPAL_RULESET = "principal-ruleset";

  public static final String OPT_OUTPUT_PRINCIPALS = "output-principals";

  private static final OptionParser parser = new OptionParser();

  private static final OptionSpec<String> optionDumperRanger =
      parser
          .accepts(
              OPT_DUMPER_RANGER,
              "Local or GCS path to the Ranger dumper output. It can be a ZIP file or unzipped directory.")
          .withRequiredArg()
          .describedAs("/path/to/ranger-dumper-output.zip");

  private static final OptionSpec<String> optionDumperHdfs =
      parser
          .accepts(
              OPT_DUMPER_HDFS,
              "Local or GCS path to the HDFS dumper output. It can be a ZIP file or unzipped directory.")
          .withRequiredArg()
          .describedAs("/path/to/hdfs-dumper-output.zip");

  private static final OptionSpec<String> optionPrincipalRuleset =
      parser
          .accepts(OPT_PRINCIPAL_RULESET, "Local or GCS path to the principal ruleset yaml file.")
          .withRequiredArg()
          .describedAs("/path/to/principal-ruleset.yaml")
          .required();
  private static final OptionSpec<String> optionOutputPrincipals =
      parser
          .accepts(OPT_OUTPUT_PRINCIPALS, "Local or GCS path to the output principals.yaml file.")
          .withRequiredArg()
          .defaultsTo("principals.yaml")
          .describedAs("/path/to/principals.yaml");

  private static final CommandArgsHelp help = new CommandArgsHelp(parser);

  private final OptionSet options;

  public ExpandOptions(String[] args) {
    options = parser.parse(args);
  }

  public boolean handleHelp() throws IOException {
    return help.handle(options);
  }

  public boolean hasDumperRanger() {
    return options.has(optionDumperRanger);
  }

  @Nonnull
  public String getDumperRanger() {
    return options.valueOf(optionDumperRanger);
  }

  public boolean hasDumperHdfs() {
    return options.has(optionDumperHdfs);
  }

  @Nonnull
  public String getDumperHdfs() {
    return options.valueOf(optionDumperHdfs);
  }

  @Nonnull
  public String getPrincipalRuleset() {
    return options.valueOf(optionPrincipalRuleset);
  }

  @Nonnull
  public String getOutputPrincipals() {
    return options.valueOf(optionOutputPrincipals);
  }
}
