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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

/** @author shevek */
public class ConnectorArguments extends DefaultArguments {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorArguments.class);

  private final String HELP_INFO =
      "The CompilerWorks Metadata Exporters address three goals:\n"
          + "\n"
          + "    1) Extract well-formatted metadata and logs for CompilerWorks suite\n"
          + "    2) Support the user with comprehensive reference data\n"
          + "    3) Provide diagnostics to CompilerWorks when debugging issues\n"
          + "\n"
          + "The exporter queries system tables for DDL related to user and system\n"
          + "databases. These are zipped into a convenient package.\n"
          + "\n"
          + "At no point are the contents of user databases themselves queried.\n"
          + "\n";

  public static final String OPT_DRIVER = "driver";
  public static final String OPT_CLASS = "jdbcDriverClass";
  public static final String OPT_URI = "url";
  public static final String OPT_HOST = "host";
  public static final String OPT_HOST_DEFAULT = "localhost";
  public static final String OPT_PORT = "port";
  public static final int OPT_PORT_ORDER = 200;
  public static final String OPT_USER = "user";
  public static final String OPT_PASSWORD = "password";
  public static final String OPT_ROLE = "role";
  public static final String OPT_WAREHOUSE = "warehouse";
  public static final String OPT_DATABASE = "database";
  public static final String OPT_SCHEMA = "schema";
  public static final String OPT_ASSESSMENT = "assessment";
  public static final String OPT_ORACLE_SID = "oracle-sid";
  public static final String OPT_ORACLE_SERVICE = "oracle-service";

  public static final String OPT_QUERY_LOG_DAYS = "query-log-days";
  public static final String OPT_QUERY_LOG_START = "query-log-start";
  public static final String OPT_QUERY_LOG_END = "query-log-end";

  // redshift.
  public static final String OPT_IAM_ACCESSKEYID = "iam-accesskeyid";
  public static final String OPT_IAM_SECRETACCESSKEY = "iam-secretaccesskey";
  public static final String OPT_IAM_PROFILE = "iam-profile";

  // Hive metastore
  public static final String OPT_HIVE_METASTORE_PORT_DEFAULT = "9083";
  public static final String OPT_HIVE_METASTORE_VERSION = "hive-metastore-version";
  public static final String OPT_HIVE_METASTORE_VERSION_DEFAULT = "2.3.6";
  public static final String OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA =
      "hive-metastore-dump-partition-metadata";
  public static final String OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA_DEFAULT = "true";

  public static final String OPT_REQUIRED_IF_NOT_URL = "if --url is not specified";
  public static final String OPT_THREAD_POOL_SIZE = "thread-pool-size";
  // These are blocking threads on the client side, so it doesn't really matter much.
  public static final Integer OPT_THREAD_POOL_SIZE_DEFAULT = 32;

  private final OptionSpec<String> connectorNameOption =
      parser.accepts("connector", "Target DBMS connector name").withRequiredArg().required();
  private final OptionSpec<String> optionDriver =
      parser
          .accepts(
              OPT_DRIVER,
              "JDBC driver path(s) (usually a proprietary JAR file distributed by the vendor)")
          .withRequiredArg()
          .withValuesSeparatedBy(',')
          .describedAs("/path/to/file.jar[,...]");
  private final OptionSpec<String> optionDriverClass =
      parser
          .accepts(OPT_CLASS, "JDBC driver class (if given, overrides the builtin default)")
          .withRequiredArg()
          .describedAs("com.company.Driver");
  private final OptionSpec<String> optionUri =
      parser
          .accepts(OPT_URI, "JDBC driver URI (overrides host, port, etc if given)")
          .withRequiredArg()
          .describedAs("jdbc:dbname:host/db?param0=foo");
  private final OptionSpec<String> optionHost =
      parser.accepts(OPT_HOST, "Database hostname").withRequiredArg().defaultsTo(OPT_HOST_DEFAULT);
  private final OptionSpec<Integer> optionPort =
      parser
          .accepts(OPT_PORT, "Database port")
          .withRequiredArg()
          .ofType(Integer.class)
          .describedAs("port");
  private final OptionSpec<String> optionWarehouse =
      parser
          .accepts(
              OPT_WAREHOUSE,
              "Virtual warehouse to use once connected (for providers such as Snowflake)")
          .withRequiredArg()
          .ofType(String.class);
  private final OptionSpec<String> optionDatabase =
      parser
          .accepts(OPT_DATABASE, "Database(s) to export")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',')
          .describedAs("db0,db1,...");
  private final OptionSpec<String> optionSchema =
      parser
          .accepts(OPT_SCHEMA, "Schemata to export")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',')
          .describedAs("sch0,sch1,...");
  private final OptionSpec<Void> optionAssessment =
      parser.accepts(
          OPT_ASSESSMENT,
          "Whether to create a dump for assessment (i.e., dump additional information).");

  private final OptionSpec<String> optionUser =
      parser.accepts(OPT_USER, "Database username").withRequiredArg().describedAs("admin");
  private final OptionSpec<String> optionPass =
      parser
          .accepts(OPT_PASSWORD, "Database password, prompted if not provided")
          .withOptionalArg()
          .describedAs("sekr1t");
  private final OptionSpec<String> optionRole =
      parser.accepts(OPT_ROLE, "Database role").withRequiredArg().describedAs("dumper");
  private final OptionSpec<String> optionOracleService =
      parser
          .accepts(OPT_ORACLE_SERVICE, "Service name for oracle")
          .withRequiredArg()
          .describedAs("ORCL")
          .ofType(String.class);
  private final OptionSpec<String> optionOracleSID =
      parser
          .accepts(OPT_ORACLE_SID, "SID name for oracle")
          .withRequiredArg()
          .describedAs("orcl")
          .ofType(String.class);
  private final OptionSpec<String> optionConfiguration =
      parser
          .accepts("config", "Configuration for DB connector")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(';')
          .describedAs("key=val;key1=val1");
  // private final OptionSpec<String> optionDatabase = parser.accepts("database", "database (can be
  // repeated; all if not
  // specified)").withRequiredArg().describedAs("my_dbname").withValuesSeparatedBy(',');
  private final OptionSpec<File> optionOutput =
      parser
          .accepts(
              "output",
              "Output file or directory name. If the file name, along with the `.zip` extension, is not provided dumper will attempt to create the zip file with the default file name in the directory.")
          .withRequiredArg()
          .ofType(File.class)
          .describedAs("cw-dump.zip");
  private final OptionSpec<Void> optionOutputContinue =
      parser.accepts("continue", "Continues writing a previous output file.");
  // TODO: Make this be an ISO instant.
  @Deprecated
  private final OptionSpec<String> optionQueryLogEarliestTimestamp =
      parser
          .accepts(
              "query-log-earliest-timestamp",
              "UNDOCUMENTED: [Deprecated: Use "
                  + OPT_QUERY_LOG_START
                  + " and "
                  + OPT_QUERY_LOG_END
                  + "] Accepts a SQL expression that will be compared to the execution timestamp of each query log entry; entries with timestamps occurring before this expression will not be exported")
          .withRequiredArg()
          .ofType(String.class);

  private final OptionSpec<Integer> optionQueryLogDays =
      parser
          .accepts(OPT_QUERY_LOG_DAYS, "The most recent N days of query logs to export")
          .withOptionalArg()
          .ofType(Integer.class)
          .describedAs("N");

  private final OptionSpec<ZonedDateTime> optionQueryLogStart =
      parser
          .accepts(
              OPT_QUERY_LOG_START,
              "Inclusive start date for query logs to export, value will be truncated to hour")
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(
              new ZonedParser(ZonedParser.DEFAULT_PATTERN, ZonedParser.DayOffset.START_OF_DAY))
          .describedAs("2001-01-01[ 00:00:00.[000]]");
  private final OptionSpec<ZonedDateTime> optionQueryLogEnd =
      parser
          .accepts(
              OPT_QUERY_LOG_END,
              "Exclusive end date for query logs to export, value will be truncated to hour")
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(
              new ZonedParser(ZonedParser.DEFAULT_PATTERN, ZonedParser.DayOffset.END_OF_DAY))
          .describedAs("2001-01-01[ 00:00:00.[000]]");

  // This is intentionally NOT provided as a default value to the optionQueryLogEnd OptionSpec,
  // because some callers
  // such as ZonedIntervalIterable want to be able to distinguish a user-specified value from this
  // dumper-specified default.
  private final ZonedDateTime OPT_QUERY_LOG_END_DEFAULT = ZonedDateTime.now(ZoneOffset.UTC);

  private final OptionSpec<String> optionFlags =
      parser
          .accepts("test-flags", "UNDOCUMENTED: for internal testing only")
          .withRequiredArg()
          .ofType(String.class);

  // TODO: private final OptionSpec<String> optionAuth = parser.accepts("auth", "extra key=value
  // params for connector").withRequiredArg().withValuesSeparatedBy(",").forHelp();
  // pa.add_argument('auth', help="extra key=value params for connector",
  // nargs=argparse.REMAINDER, type=lambda x: x.split("="))
  // private final OptionSpec<String> optionVerbose = parser.accepts("verbose", "enable verbose
  // info").withOptionalArg().forHelp();
  // final OptionSpec<Boolean> optionAppend = parser.accepts("append", "accumulate meta from
  // multiple runs in one
  // directory").withRequiredArg().ofType(Boolean.class).defaultsTo(false).forHelp();
  private final OptionSpec<Void> optionDryrun =
      parser
          .acceptsAll(Arrays.asList("dry-run", "n"), "Show export actions without executing.")
          .forHelp();
  private final OptionSpec<String> optionQueryLogAlternates =
      parser
          .accepts(
              "query-log-alternates",
              "pair of alternate query log tables to export (teradata-logs only), by default "
                  + "logTable=dbc.DBQLogTbl and queryTable=dbc.DBQLSQLTbl, if --assessment flag"
                  + " is enabled, then logTable=dbc.QryLogV and queryTable=dbc.DBQLSQLTbl.")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',')
          .describedAs("logTable,queryTable");

  private final OptionSpec<File> optionSqlScript =
      parser
          .accepts("sqlscript", "UNDOCUMENTED: SQL Script")
          .withRequiredArg()
          .ofType(File.class)
          .describedAs("script.sql");

  // redshift.
  private final OptionSpec<String> optionRedshiftIAMAccessKeyID =
      parser.accepts(OPT_IAM_ACCESSKEYID).withRequiredArg();
  private final OptionSpec<String> optionRedshiftIAMSecretAccessKey =
      parser.accepts(OPT_IAM_SECRETACCESSKEY).withRequiredArg();
  private final OptionSpec<String> optionRedshiftIAMProfile =
      parser.accepts(OPT_IAM_PROFILE).withRequiredArg();

  // Hive metastore
  public final OptionSpec<String> optionHiveMetastoreVersion =
      parser
          .accepts(OPT_HIVE_METASTORE_VERSION)
          .withOptionalArg()
          .describedAs("major.minor.patch")
          .defaultsTo(OPT_HIVE_METASTORE_VERSION_DEFAULT);
  public final OptionSpec<Boolean> optionHivePartitionMetadataCollection =
      parser
          .accepts(OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA)
          .withOptionalArg()
          .withValuesConvertedBy(BooleanValueConverter.INSTANCE)
          .defaultsTo(Boolean.parseBoolean(OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA_DEFAULT));

  // Threading / Pooling
  private final OptionSpec<Integer> optionThreadPoolSize =
      parser
          .accepts(
              OPT_THREAD_POOL_SIZE,
              "Set thread pool size (affects connection pool size). Defaults to "
                  + OPT_THREAD_POOL_SIZE_DEFAULT)
          .withRequiredArg()
          .ofType(Integer.class)
          .defaultsTo(OPT_THREAD_POOL_SIZE_DEFAULT);

  // generic connector
  private final OptionSpec<String> optionGenericQuery =
      parser.accepts("generic-query", "Query for generic connector").withRequiredArg();
  private final OptionSpec<String> optionGenericEntry =
      parser
          .accepts("generic-entry", "Entry name in zip file for generic connector")
          .withRequiredArg();

  // Save response file
  private final OptionSpec<String> optionSaveResponse =
      parser
          .accepts(
              "save-response-file",
              "Save JSON response file, can be used in place of command line options.")
          .withOptionalArg()
          .ofType(String.class)
          .defaultsTo("dumper-response-file.json");

  // Pass properties
  private final OptionSpec<String> definitionOption =
      parser
          .accepts("D", "Pass a key=value property.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("define");
  private Map<String, String> definitionMap = null;

  // because of quoting of special characeters on command line... the -password is made optional,
  // and if not given, asked at the console.
  private String askedPassword;

  public ConnectorArguments(@Nonnull String[] args) throws IOException {
    super(args);
  }

  private static class InputDescriptor implements Comparable<InputDescriptor> {

    public static enum Category {
      Arg,
      Env,
      Other
    }

    private final RespectsInput annotation;

    public InputDescriptor(RespectsInput annotation) {
      this.annotation = annotation;
    }

    @Nonnull
    public Category getCategory() {
      if (!Strings.isNullOrEmpty(annotation.arg())) return Category.Arg;
      if (!Strings.isNullOrEmpty(annotation.env())) return Category.Env;
      return Category.Other;
    }

    @Nonnull
    public String getKey() {
      switch (getCategory()) {
        case Arg:
          return "--" + annotation.arg();
        case Env:
          return annotation.env();
        default:
          return String.valueOf(annotation.hashCode());
      }
    }

    @Override
    public int compareTo(InputDescriptor o) {
      return ComparisonChain.start()
          .compare(getCategory(), o.getCategory())
          .compare(annotation.order(), o.annotation.order())
          .result();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      String key = getKey();
      buf.append(key).append(StringUtils.repeat(' ', 12 - key.length()));
      if (getCategory() == Category.Env) buf.append(" (environment variable)");
      String defaultValue = annotation.defaultValue();
      if (!Strings.isNullOrEmpty(defaultValue))
        buf.append(" (default: ").append(defaultValue).append(")");
      buf.append(" ").append(annotation.description());
      String required = annotation.required();
      if (!Strings.isNullOrEmpty(required)) buf.append(" (Required ").append(required).append(".)");
      return buf.toString();
    }
  }

  @Nonnull
  private static Collection<InputDescriptor> getAcceptsInputs(@Nonnull Connector connector) {
    Map<String, InputDescriptor> tmp = new HashMap<>();
    Class<?> connectorType = connector.getClass();
    while (connectorType != null) {
      Set<RespectsInput> respectsInputs =
          AnnotationUtils.getDeclaredRepeatableAnnotations(connectorType, RespectsInput.class);
      // LOG.debug(connectorType + " -> " + respectsInputs);
      for (RespectsInput respectsInput : respectsInputs) {
        InputDescriptor descriptor = new InputDescriptor(respectsInput);
        tmp.putIfAbsent(descriptor.getKey(), descriptor);
      }
      connectorType = connectorType.getSuperclass();
    }

    List<InputDescriptor> out = new ArrayList<>(tmp.values());
    Collections.sort(out);
    return out;
  }

  @Override
  protected void printHelpOn(PrintStream out, OptionSet o) throws IOException {
    out.append(HELP_INFO);
    super.printHelpOn(out, o);

    // if --connector <valid-connection> provided, print only that
    if (o.has(connectorNameOption)) {
      String helpOnConnector = o.valueOf(connectorNameOption);
      for (Connector connector : ServiceLoader.load(Connector.class)) {
        if (helpOnConnector.equals(connector.getName())) {
          out.append("\nSelected connector:\n");
          printConnectorHelp(out, connector);
          return;
        }
      }
    }
    out.append("\nAvailable connectors:\n");
    for (Connector connector : ServiceLoader.load(Connector.class)) {
      printConnectorHelp(out, connector);
    }
  }

  private void printConnectorHelp(@Nonnull Appendable out, @Nonnull Connector connector)
      throws IOException {
    Description description = connector.getClass().getAnnotation(Description.class);
    out.append("* " + connector.getName());
    if (description != null) out.append(" - ").append(description.value());
    out.append("\n");
    for (InputDescriptor descriptor : getAcceptsInputs(connector))
      out.append("        ").append(descriptor.toString()).append("\n");
    for (Enum<? extends ConnectorProperty> enumConstant :
        connector.getConnectorProperties().getEnumConstants()) {
      ConnectorProperty property = (ConnectorProperty) enumConstant;
      out.append("        ")
          .append("-D")
          .append(property.getName())
          .append("=value")
          .append("\t\t")
          .append(property.getDescription())
          .append("\n");
    }
  }

  @Nonnull
  public String getConnectorName() {
    return getOptions().valueOf(connectorNameOption);
  }

  @CheckForNull
  public List<String> getDriverPaths() {
    return getOptions().valuesOf(optionDriver);
  }

  @Nonnull
  public String getDriverClass(@Nonnull String defaultDriverClass) {
    return MoreObjects.firstNonNull(getOptions().valueOf(optionDriverClass), defaultDriverClass);
  }

  @CheckForNull
  public String getDriverClass() {
    return getOptions().valueOf(optionDriverClass);
  }

  @CheckForNull
  public String getUri() {
    return getOptions().valueOf(optionUri);
  }

  @Nonnull
  public String getHost() {
    return getOptions().valueOf(optionHost);
  }

  @Nonnull
  public String getHost(@Nonnull String defaultHost) {
    return MoreObjects.firstNonNull(getHost(), defaultHost);
  }

  @CheckForNull
  public Integer getPort() {
    return getOptions().valueOf(optionPort);
  }

  @Nonnegative
  public int getPort(@Nonnegative int defaultPort) {
    Integer customPort = getPort();
    if (customPort != null) return customPort.intValue();
    return defaultPort;
  }

  @CheckForNull
  public String getWarehouse() {
    return getOptions().valueOf(optionWarehouse);
  }

  @CheckForNull
  public String getOracleServicename() {
    return getOptions().valueOf(optionOracleService);
  }

  @CheckForNull
  public String getOracleSID() {
    return getOptions().valueOf(optionOracleSID);
  }

  @Nonnull
  private static Predicate<String> toPredicate(@CheckForNull List<String> in) {
    if (in == null || in.isEmpty()) return Predicates.alwaysTrue();
    return Predicates.in(new HashSet<>(in));
  }

  @Nonnull
  public List<String> getDatabases() {
    return getOptions().valuesOf(optionDatabase).stream()
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .collect(toImmutableList());
  }

  @Nonnull
  public Predicate<String> getDatabasePredicate() {
    return toPredicate(getDatabases());
  }

  /** Returns the name of the single database specified, if exactly one database was specified. */
  // This can be used to generate an output filename, but it makes 1 be a special case
  // that I find a little uncomfortable from the Unix philosophy:
  // "Sometimes the output filename is different" is hard to automate around.
  @CheckForNull
  public String getDatabaseSingleName() {
    List<String> databases = getDatabases();
    if (databases.size() == 1) return databases.get(0);
    else return null;
  }

  @Nonnull
  public List<String> getSchemata() {
    return getOptions().valuesOf(optionSchema);
  }

  public boolean isAssessment() {
    return getOptions().has(optionAssessment);
  }

  private <T> Optional<T> optionAsOptional(OptionSpec<T> spec) {
    if (!getOptions().has(spec)) {
      return Optional.empty();
    }
    return Optional.of(getOptions().valueOf(spec));
  }

  @Nonnull
  public Predicate<String> getSchemaPredicate() {
    return toPredicate(getSchemata());
  }

  @CheckForNull
  public String getUser() {
    return getOptions().valueOf(optionUser);
  }

  // -password has optional argument, and if not provied
  // should be asked from command line
  @CheckForNull
  public String getPassword() {
    if (!getOptions().has(optionPass)) return null;
    String pass = getOptions().valueOf(optionPass);
    if (pass != null) return pass;
    // Else need to ask & save.
    if (askedPassword != null) return askedPassword;

    Console console = System.console();
    if (console == null) {
      LOG.info("Cannot prompt for password, Console not available");
      return null;
    } else {
      console.printf("Password: ");
      pass = new String(console.readPassword());
      askedPassword = pass;
      return askedPassword;
    }
  }

  @CheckForNull
  public String getRole() {
    return getOptions().valueOf(optionRole);
  }

  @Nonnull
  public List<String> getConfiguration() {
    return getOptions().valuesOf(optionConfiguration);
  }

  public Optional<File> getOutputFile() {
    return optionAsOptional(optionOutput).filter(file -> !file.getName().isEmpty());
  }

  public boolean isOutputContinue() {
    return getOptions().has(optionOutputContinue);
  }

  public boolean isDryRun() {
    return getOptions().has(optionDryrun);
  }

  @CheckForNull
  @Deprecated
  public String getQueryLogEarliestTimestamp() {
    return getOptions().valueOf(optionQueryLogEarliestTimestamp);
  }

  @CheckForNull
  public Integer getQueryLogDays() {
    return getOptions().valueOf(optionQueryLogDays);
  }

  @Nonnegative
  public int getQueryLogDays(@Nonnegative int defaultQueryLogDays) {
    Integer out = getQueryLogDays();
    if (out != null) return out.intValue();
    return defaultQueryLogDays;
  }

  /**
   * Get the inclusive starting datetime for query log extraction.
   *
   * @return a nullable zoned datetime
   */
  @CheckForNull
  public ZonedDateTime getQueryLogStart() {
    return getOptions().valueOf(optionQueryLogStart);
  }

  /**
   * Get the exclusive ending datetime for query log extraction.
   *
   * @return a nullable zoned datetime
   */
  @CheckForNull
  public ZonedDateTime getQueryLogEnd() {
    return getOptions().valueOf(optionQueryLogEnd);
  }

  /**
   * Get the exclusive ending datetime for query log extraction; if not specified by the user,
   * returns the value of {@link ZonedDateTime#now()} at the time this {@link ConnectorArguments}
   * instance was instantiated.
   *
   * <p>Repeated calls to this method always yield the same value.
   *
   * @return a non-null zoned datetime
   */
  @Nonnull
  public ZonedDateTime getQueryLogEndOrDefault() {
    return MoreObjects.firstNonNull(
        getOptions().valueOf(optionQueryLogEnd), OPT_QUERY_LOG_END_DEFAULT);
  }

  @CheckForNull
  public List<String> getQueryLogAlternates() {
    return getOptions().valuesOf(optionQueryLogAlternates);
  }

  public boolean isTestFlag(char c) {
    String flags = getOptions().valueOf(optionFlags);
    if (flags == null) return false;
    return flags.indexOf(c) >= 0;
  }

  @CheckForNull
  public File getSqlScript() {
    return getOptions().valueOf(optionSqlScript);
  }

  @CheckForNull
  public String getIAMAccessKeyID() {
    return getOptions().valueOf(optionRedshiftIAMAccessKeyID);
  }

  @CheckForNull
  public String getIAMSecretAccessKey() {
    return getOptions().valueOf(optionRedshiftIAMSecretAccessKey);
  }

  @CheckForNull
  public String getIAMProfile() {
    return getOptions().valueOf(optionRedshiftIAMProfile);
  }

  public int getThreadPoolSize() {
    return getOptions().valueOf(optionThreadPoolSize);
  }

  @CheckForNull
  public String getGenericQuery() {
    return getOptions().valueOf(optionGenericQuery);
  }

  @CheckForNull
  public String getGenericEntry() {
    return getOptions().valueOf(optionGenericEntry);
  }

  @Nonnull
  public String getHiveMetastoreVersion() {
    return getOptions().valueOf(optionHiveMetastoreVersion);
  }

  public boolean isHiveMetastorePartitionMetadataDumpingEnabled() {
    return BooleanUtils.isTrue(getOptions().valueOf(optionHivePartitionMetadataCollection));
  }

  public boolean saveResponseFile() {
    return getOptions().has(optionSaveResponse);
  }

  @Nonnull
  public String getResponseFileName() {
    return getOptions().valueOf(optionSaveResponse);
  }

  @CheckForNull
  public String getDefinition(@Nonnull ConnectorProperty property) {
    if (definitionMap == null) searchDefinitions();
    String result = definitionMap.get(property.getName());
    LOG.info(
        "Retrieving {} from key {} - got: {}",
        property.getName(),
        property.getDescription(),
        MoreObjects.firstNonNull(result, "[not found]"));
    return result;
  }

  public void searchDefinitions() {
    definitionMap = new HashMap<>();
    for (String definition : getOptions().valuesOf(definitionOption)) {
      if (definition.contains("=")) {
        int idx = definition.indexOf("=");
        definitionMap.put(definition.substring(0, idx), definition.substring(idx + 1));
      }
    }
  }

  @Override
  @Nonnull
  public String toString() {
    // We do not include password here b/c as of this writing,
    // this string representation is logged out to file by ArgumentsTask.
    return MoreObjects.toStringHelper(this)
        .add("connector", getConnectorName())
        .add("driver", getDriverPaths())
        .add("host", getHost())
        .add("port", getPort())
        .add("warehouse", getWarehouse())
        .add("database", getDatabases())
        .add("user", getUser())
        .add("configuration", getConfiguration())
        .add("output", getOutputFile())
        .add("query-log-earliest-timestamp", getQueryLogEarliestTimestamp())
        .add("query-log-days", getQueryLogDays())
        .add("query-log-start", getQueryLogStart())
        .add("query-log-end", getQueryLogEnd())
        .add("query-log-alternates", getQueryLogAlternates())
        .add("assessment", isAssessment())
        .toString();
  }

  public static class ZonedParser implements ValueConverter<ZonedDateTime> {

    public static final String DEFAULT_PATTERN = "yyyy-MM-dd[ HH:mm:ss[.SSS]]";
    private final DayOffset dayOffset;
    private final DateTimeFormatter parser;

    public ZonedParser(String pattern, DayOffset dayOffset) {
      this.dayOffset = dayOffset;
      this.parser =
          DateTimeFormatter.ofPattern(pattern, Locale.US).withResolverStyle(ResolverStyle.LENIENT);
    }

    @Override
    public ZonedDateTime convert(String value) {

      TemporalAccessor result = parser.parseBest(value, LocalDateTime::from, LocalDate::from);

      if (result instanceof LocalDateTime) return ((LocalDateTime) result).atZone(ZoneOffset.UTC);

      if (result instanceof LocalDate)
        return ((LocalDate) result)
            .plusDays(dayOffset.getValue())
            .atTime(LocalTime.MIDNIGHT)
            .atZone(ZoneOffset.UTC);

      throw new ValueConversionException(
          "Value " + value + " cannot be parsed to date or datetime");
    }

    @Override
    public Class<ZonedDateTime> valueType() {
      return ZonedDateTime.class;
    }

    @Override
    public String valuePattern() {
      return null;
    }

    public enum DayOffset {
      START_OF_DAY(0L),
      END_OF_DAY(1L);

      private final long value;

      DayOffset(long value) {
        this.value = value;
      }

      public long getValue() {
        return value;
      }
    }
  }
}
