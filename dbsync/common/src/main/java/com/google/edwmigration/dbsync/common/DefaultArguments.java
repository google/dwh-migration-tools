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
package com.google.edwmigration.dbsync.common;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import javax.annotation.Nonnull;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;
import org.anarres.jdiagnostics.ProductMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultArguments {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArguments.class);

  protected final OptionParser parser = new OptionParser();
  private final OptionSpec<?> helpOption =
      parser.accepts("help", "Displays command-line help.").forHelp();
  private final OptionSpec<?> versionOption =
      parser.accepts("version", "Displays the product version and exits.").forHelp();
  private final String[] args;
  private OptionSet options;

  @SuppressWarnings("EI_EXPOSE_REP2")
  public DefaultArguments(@Nonnull String[] args) {
    this.args = args;
  }

  @Nonnull
  @SuppressWarnings("EI_EXPOSE_REP")
  public String[] getArgs() {
    return args;
  }

  protected void printHelpOn(@Nonnull PrintStream out, OptionSet o) throws IOException {
    parser.printHelpOn(out);
  }

  @Nonnull
  @SuppressWarnings("DM_EXIT")
  protected OptionSet parseOptions() throws Exception {
    OptionSet o = parser.parse(args);
    if (o.has(helpOption)) {
      printHelpOn(System.err, o);
      System.exit(1);
    }
    if (o.has(versionOption)) {
      System.err.println(new ProductMetadata());
      System.exit(0);
    }
    return o;
  }

  @Nonnull
  public OptionSet getOptions() {
    if (options == null) {
      try {
        options = parseOptions();
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
    return options;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("args", Arrays.toString(args)).toString();
  }
}
