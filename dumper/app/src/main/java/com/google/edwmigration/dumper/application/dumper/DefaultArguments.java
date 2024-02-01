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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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
import org.apache.commons.lang3.StringUtils;
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

  public static class DurationValueConverter implements ValueConverter<Duration> {

    private enum AllowedUnits {
      HOUR(HOURS, "hourly"),
      DAY(DAYS, "daily");

      private final ChronoUnit chronoUnit;
      private final String commandLineFlag;

      AllowedUnits(ChronoUnit chronoUnit, String commandLineFlag) {
        this.chronoUnit = chronoUnit;
        this.commandLineFlag = commandLineFlag;
      }
    }

    public static DurationValueConverter INSTANCE = new DurationValueConverter();

    private DurationValueConverter() {}

    @Override
    public Duration convert(String value) {
      for (AllowedUnits unit : AllowedUnits.values()) {
        if (unit.commandLineFlag.equals(value)) {
          return unit.chronoUnit.getDuration();
        }
      }
      throw new ValueConversionException("Not a valid unit of interval: " + value);
    }

    @Override
    public Class<? extends Duration> valueType() {
      return Duration.class;
    }

    @Override
    public String valuePattern() {
      return Arrays.stream(AllowedUnits.values())
          .map(unit -> unit.commandLineFlag)
          .collect(Collectors.joining(", "));
    }
  }

  public enum HadoopRpcProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");

    private final String qopValue;

    HadoopRpcProtection(String qopValue) {
      this.qopValue = qopValue;
    }
  }

  public interface Converter<V> {
    Optional<V> convert(String value);
  }

  public static class HadoopSaslQopConverter implements Converter<String> {

    public static HadoopSaslQopConverter INSTANCE = new HadoopSaslQopConverter();

    private HadoopSaslQopConverter() {}

    @Override
    public Optional<String> convert(String value) throws MetadataDumperUsageException {
      if (StringUtils.isEmpty(value)) {
        return Optional.empty();
      }

      for (HadoopRpcProtection qop : HadoopRpcProtection.values()) {
        if (qop.name().equalsIgnoreCase(value)) {
          return Optional.of(qop.qopValue);
        }
      }
      throw new MetadataDumperUsageException("Not a valid QOP: " + value);
    }
  }

  private static final String PRODUCT_GROUP = "com.google.edwmigration.dumper";
  private static final String PRODUCT_CORE_MODULE = "app";

  protected final OptionParser parser = new OptionParser();
  private final OptionSpec<?> helpOption =
      parser.accepts("help", "Displays command-line help.").forHelp();
  private final OptionSpec<?> versionOption =
      parser.accepts("version", "Displays the product version and exits.").forHelp();
  protected static final String OPT_CONNECTOR = "connector";

  protected final OptionSpec<String> connectorNameOption =
      parser.accepts(OPT_CONNECTOR, "Target DBMS connector name").withRequiredArg().required();

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
  public String getConnectorName() {
    return getOptions().valueOf(connectorNameOption);
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
        ImmutableSetMultimap<String, ConnectorProperty> properties = allPropertiesByConnector();
        addConnectorSpecificProperties(properties);
        options = parseOptions();
        validateConnectorSpecificProperties(properties, options.valueOf(connectorNameOption));
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
    return options;
  }

  private void validateConnectorSpecificProperties(
      ImmutableSetMultimap<String, ConnectorProperty> properties, String connectorName) {
    properties.entries().stream()
        .filter(entry -> !entry.getKey().equals(connectorName))
        .map(Entry::getValue)
        .map(DefaultArguments::transformToOption)
        .filter(options::has)
        .findFirst()
        .ifPresent(
            option -> {
              throw new MetadataDumperUsageException(
                  String.format(
                      "Property: name='%s', value='%s' is not compatible with connector '%s'",
                      option, options.valueOf(option), connectorName));
            });
  }

  protected static String transformToOption(ConnectorProperty property) {
    return "D" + property.getName();
  }

  private void addConnectorSpecificProperties(
      ImmutableSetMultimap<String, ConnectorProperty> properties) {
    properties
        .values()
        .forEach(
            property ->
                parser
                    .accepts("D" + property.getName(), property.getDescription())
                    .withRequiredArg());
  }

  private static ImmutableSetMultimap<String, ConnectorProperty> allPropertiesByConnector() {
    ImmutableSetMultimap.Builder<String, ConnectorProperty> connectorPropertyNames =
        ImmutableSetMultimap.builder();
    for (Connector connector : ConnectorRepository.getInstance().getAllConnectors()) {
      String connectorName = connector.getName();
      for (Enum<? extends ConnectorProperty> enumConstant :
          connector.getConnectorProperties().getEnumConstants()) {
        connectorPropertyNames.put(connectorName, ((ConnectorProperty) enumConstant));
      }
    }
    return connectorPropertyNames.build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("args", Arrays.toString(args)).toString();
  }
}
