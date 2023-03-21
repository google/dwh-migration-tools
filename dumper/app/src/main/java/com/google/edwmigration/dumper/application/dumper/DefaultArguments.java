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
package com.google.edwmigration.dumper.application.dumper;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionDescriptor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;
import org.anarres.jdiagnostics.ProductMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public class DefaultArguments {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(DefaultArguments.class);

  public static class BooleanValueConverter implements ValueConverter<Boolean> {

    private final String[] V_TRUE = {"true", "t", "yes", "y", "1"};
    private final String[] V_FALSE = {"false", "f", "no", "n", "0"};

    public static BooleanValueConverter INSTANCE = new BooleanValueConverter();

    private BooleanValueConverter() {}

    @Override
    public Boolean convert(String value) {
      for (String s : V_TRUE) if (value.equalsIgnoreCase(s)) return Boolean.TRUE;
      for (String s : V_FALSE) if (value.equalsIgnoreCase(s)) return Boolean.FALSE;
      throw new ValueConversionException("Not a valid boolean value: " + value);
    }

    @Override
    public Class<? extends Boolean> valueType() {
      return Boolean.class;
    }

    @Override
    public String valuePattern() {
      StringBuilder buf = new StringBuilder();
      Joiner joiner = Joiner.on('/');
      joiner.appendTo(buf, V_TRUE);
      buf.append('/');
      joiner.appendTo(buf, V_FALSE);
      return buf.toString();
    }
  }

  private static final String PRODUCT_GROUP = "com.google.edwmigration.dumper";
  private static final String PRODUCT_CORE_MODULE = "app";

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

  // Any description starting with UNDOCUMENTED: is ... undocumented
  protected void printHelpOn(@Nonnull PrintStream out, OptionSet o) throws IOException {

    BuiltinHelpFormatter helpFormatter =
        new BuiltinHelpFormatter(120, 4) {
          @Override
          public String format(Map<String, ? extends OptionDescriptor> options) {

            // https://github.com/jopt-simple/jopt-simple/blob/master/src/main/java/joptsimple/BuiltinHelpFormatter.java#L91
            Comparator<OptionDescriptor> comparator =
                (first, second) ->
                    first.options().iterator().next().compareTo(second.options().iterator().next());
            Set<OptionDescriptor> sorted = new TreeSet<>(comparator);

            sorted.addAll(
                options.values().stream()
                    .filter((val) -> !val.description().startsWith("UNDOCUMENTED:"))
                    .collect(Collectors.toList()));

            this.addRows(sorted);
            return this.formattedHelpOutput();
          }
        };
    parser.formatHelpWith(helpFormatter);

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
      System.err.println(
          new ProductMetadata().getModule(PRODUCT_GROUP + ":" + PRODUCT_CORE_MODULE));
      System.exit(1);
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
