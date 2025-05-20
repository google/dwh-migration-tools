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
package com.google.edwmigration.permissions.commands.batch;

import com.google.edwmigration.permissions.commands.CommandArgsHelp;
import java.io.IOException;
import javax.annotation.Nonnull;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class BatchOptions {

  private static final OptionParser parser = new OptionParser();

  private static final OptionSpec<String> optionSourcePath =
      parser
          .accepts("source-path", "GCS path to tables metadata, should end with slash /.")
          .withRequiredArg()
          .describedAs("gs://<source-bucket>/tables/")
          .required();

  private static final OptionSpec<String> optionTargetPath =
      parser
          .accepts("target-path", "GCS path to batch tables metadata, should end with slash /.")
          .withRequiredArg()
          .describedAs("gs://<source-bucket>/batch/BATCH_NAME/")
          .required();

  private static final OptionSpec<String> optionPattern =
      parser
          .accepts("pattern", "RE2 regex pattern of GCS objects to copy.")
          .withRequiredArg()
          .describedAs(".*")
          .required();

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

  public BatchOptions(String[] args) {
    options = parser.parse(args);
  }

  public boolean handleHelp() throws IOException {
    return help.handle(options);
  }

  @Nonnull
  public String getSourcePath() {
    return options.valueOf(optionSourcePath);
  }

  @Nonnull
  public String getTargetPath() {
    return options.valueOf(optionTargetPath);
  }

  public int getNumThreads() {
    return options.valueOf(optionNumThreads);
  }

  public int getTimeoutSeconds() {
    return options.valueOf(optionTimeoutSeconds);
  }

  @Nonnull
  public String getPattern() {
    return options.valueOf(optionPattern);
  }
}
