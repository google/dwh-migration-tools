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
package com.google.edwmigration.dumper.application.dumper;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ZonedParser.DayOffset;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
import com.google.edwmigration.dumper.application.dumper.io.PasswordReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/** @author shevek */
public class ConnectorArguments extends DefaultArguments {

  private static final String HELP_INFO =
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

  public static final String OPT_CONNECTOR = "connector";
  public static final String OPT_TELEMETRY = "telemetry";
  public static final String OPT_DRIVER = "driver";
  public static final String OPT_CLASS = "jdbcDriverClass";
  public static final String OPT_URI = "url";
  public static final String OPT_HOST = "host";
  public static final String OPT_HOST_DEFAULT = "localhost";
  public static final String OPT_PORT = "port";
  public static final int OPT_PORT_ORDER = 200;
  public static final String OPT_USER = "user";
  public static final String OPT_PASSWORD = "password";
  public static final String OPT_START_DATE = "start-date";
  public static final String OPT_START_DATE_DESC =
      "Inclusive start date for data to export, value will be truncated to hour.";
  public static final String OPT_END_DATE = "end-date";
  public static final String OPT_END_DATE_DESC =
      "Exclusive end date for data to export, value will be truncated to hour.";

  public static final String OPT_CLUSTER = "cluster";
  public static final String OPT_ROLE = "role";
  public static final String OPT_WAREHOUSE = "warehouse";
  public static final String OPT_DATABASE = "database";
  public static final String OPT_SCHEMA = "schema";
  public static final String OPT_OUTPUT = "output";
  public static final String OPT_CONFIG = "config";
  public static final String OPT_ASSESSMENT = "assessment";
  public static final String OPT_ORACLE_SID = "oracle-sid";
  public static final String OPT_ORACLE_SERVICE = "oracle-service";

  public static final String OPT_QUERY_LOG_DAYS = "query-log-days";
  public static final String OPT_QUERY_LOG_ROTATION_FREQUENCY = "query-log-rotation-frequency";
  public static final String OPT_QUERY_LOG_START = "query-log-start";
  public static final String OPT_QUERY_LOG_END = "query-log-end";
  public static final String OPT_QUERY_LOG_EARLIEST_TIMESTAMP = "query-log-earliest-timestamp";
  public static final String OPT_QUERY_LOG_ALTERNATES = "query-log-alternates";

  // Snowflake
  public static final String OPT_PRIVATE_KEY_FILE = "private-key-file";
  public static final String OPT_PRIVATE_KEY_PASSWORD = "private-key-password";

  // Cloudera
  public static final String OPT_YARN_APPLICATION_TYPES = "yarn-application-types";
  public static final String OPT_PAGINATION_PAGE_SIZE = "pagination-page-size";

  // redshift.
  public static final String OPT_IAM_ACCESSKEYID = "iam-accesskeyid";
  public static final String OPT_IAM_SECRETACCESSKEY = "iam-secretaccesskey";
  public static final String OPT_IAM_SESSIONTOKEN = "iam-sessiontoken";
  public static final String OPT_IAM_PROFILE = "iam-profile";

  // Port 8020 is used by HDFS to communicate with the NameNode.
  public static final String OPT_HDFS_PORT_DEFAULT = "8020";

  // Hive metastore
  public static final String OPT_HIVE_METASTORE_PORT_DEFAULT = "9083";
  public static final String OPT_HIVE_METASTORE_VERSION = "hive-metastore-version";
  public static final String OPT_HIVE_METASTORE_VERSION_DEFAULT = "2.3.6";
  public static final String OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA =
      "hive-metastore-dump-partition-metadata";
  public static final String OPT_HIVE_METASTORE_DUMP_PARTITION_METADATA_DEFAULT = "true";
  public static final String OPT_HIVE_KERBEROS_URL = "hive-kerberos-url";
  public static final String OPT_REQUIRED_IF_NOT_URL = "if --url is not specified";
  public static final String OPT_THREAD_POOL_SIZE = "thread-pool-size";
  public static final String OPT_KERBEROS_AUTH_FOR_HADOOP = "kerberos-auth-for-hadoop";
  public static final String OPT_HADOOP_CORE_SITE_XML = "hadoop-core-site-xml";
  public static final String OPT_HADOOP_HDFS_SITE_XML = "hadoop-hdfs-site-xml";
  public static final String OPT_KERBEROS_KEYTAB_PATH = "kerberos-keytab-path";
  public static final String OPT_KERBEROS_PRINCIPAL = "kerberos-principal";

  public static final String OPT_HADOOP_RPC_PROTECTION = "hadoop-rpc-protection";

  public static final String OPT_HDFS_PRINCIPAL_PREFIX = "hdfs-principal-prefix";

  public static final String OPT_HDFS_SCAN_ROOT_PATH = "hdfs-scan-root-path";
  public static final String OPT_HADOOP_CORE_SITE_XML_DEFAULT =
      "/etc/hadoop/conf.cloudera.hdfs/core-site.xml";
  public static final String OPT_HADOOP_HDFS_SITE_XML_DEFAULT =
      "/etc/hadoop/conf.cloudera.hdfs/hdfs-site.xml";
  public static final String OPT_HDFS_PRINCIPAL_PREFIX_DEFAULT = "hdfs/_HOST@";
  public static final String OPT_HDFS_SCAN_ROOT_PATH_DEFAULT = "/";
  // Ranger.
  public static final String OPT_RANGER_PORT_DEFAULT = "6080";
  public static final String OPT_RANGER_PAGE_SIZE = "ranger-page-size";
  public static final int OPT_RANGER_PAGE_SIZE_DEFAULT = 1000;
  public static final String OPT_RANGER_SCHEME = "ranger-scheme";
  public static final String OPT_RANGER_SCHEME_DEFAULT = "http";
  public static final String OPT_RANGER_DISABLE_TLS_VALIDATION = "ranger-disable-tls-validation";

  // These are blocking threads on the client side, so it doesn't really matter
  // much.
  public static final Integer OPT_THREAD_POOL_SIZE_DEFAULT = 32;

  private final OptionSpec<String> connectorNameOption =
      parser.accepts(OPT_CONNECTOR, "Target connector name").withRequiredArg().required();
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
          .accepts(
              OPT_URI,
              "The main Connector's URI to dump metadata. "
                  + "It can ba a JDBC driver URI or a HTTP endpoint URL, must be specified "
                  + "by connector.")
          .withRequiredArg()
          .describedAs(
              "JDBC looks like: [jdbc:dbname:host/db?param0=foo], "
                  + "HTTP looks like: [http://localhost:8080/api/v1/]");
  private final OptionSpec<String> optionHost =
      parser.accepts(OPT_HOST, "Database hostname").withRequiredArg();
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

  private final OptionSpec<String> optionPrivateKeyFile =
      parser
          .accepts(OPT_PRIVATE_KEY_FILE, "Path to the Private Key file used for authentication.")
          .withRequiredArg()
          .describedAs("/path/to/ras_key.p8");
  private final OptionSpec<String> optionPrivateKeyPassword =
      parser
          .accepts(
              OPT_PRIVATE_KEY_PASSWORD, "Private Key file password. Required if file is encrypted.")
          .withRequiredArg()
          .describedAs("sekr1t");

  private final OptionSpec<ZonedDateTime> optionStartDate =
      parser
          .accepts(OPT_START_DATE, OPT_START_DATE_DESC)
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(ZonedParser.withDefaultPattern(DayOffset.START_OF_DAY))
          .describedAs("2001-01-15[ 00:00:00.[000]]");

  private final OptionSpec<ZonedDateTime> optionEndDate =
      parser
          .accepts(OPT_END_DATE, OPT_END_DATE_DESC)
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(ZonedParser.withDefaultPattern(DayOffset.START_OF_DAY))
          .describedAs("2001-01-15[ 00:00:00.[000]]");

  private final OptionSpec<String> optionCluster =
      parser
          .accepts(OPT_CLUSTER, "Cluster name to dump metadata")
          .withOptionalArg()
          .describedAs("name")
          .ofType(String.class);
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
          .accepts(OPT_CONFIG, "Configuration for DB connector")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(';')
          .describedAs("key=val;key1=val1");
  private final OptionSpec<String> optionOutput =
      parser
          .accepts(
              OPT_OUTPUT,
              "Output file, directory name, or GCS path. If the file name, along with "
                  + "the `.zip` extension, is not provided dumper will attempt to create the zip "
                  + "file with the default file name in the directory. To use GCS, use the format "
                  + "gs://<BUCKET>/<PATH>. This requires Google Cloud credentials. See "
                  + "https://cloud.google.com/docs/authentication/client-libraries for details.")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs("cw-dump.zip");
  private final OptionSpec<Void> optionOutputContinue =
      parser.accepts("continue", "Continues writing a previous output file.");

  /**
   * (Deprecated) earliest timestamp of logs to extract.
   *
   * <p>If the user specifies an earliest start time there will be extraneous empty dump files
   * because we always iterate over the full 7 trailing days; maybe it's worth preventing that in
   * the future. To do that, we should require getQueryLogEarliestTimestamp() to parse and return an
   * ISO instant, not a database-server-specific format.
   */
  @Deprecated
  private final OptionSpec<String> optionQueryLogEarliestTimestamp =
      parser
          .accepts(
              OPT_QUERY_LOG_EARLIEST_TIMESTAMP,
              "UNDOCUMENTED: [Deprecated: Use "
                  + OPT_QUERY_LOG_START
                  + " and "
                  + OPT_QUERY_LOG_END
                  + "] Accepts a SQL expression that will be compared to the execution timestamp of"
                  + " each query log entry; entries with timestamps occurring before this"
                  + " expression will not be exported")
          .withRequiredArg()
          .ofType(String.class);

  private final OptionSpec<Integer> optionQueryLogDays =
      parser
          .accepts(OPT_QUERY_LOG_DAYS, "The most recent N days of query logs to export")
          .withOptionalArg()
          .ofType(Integer.class)
          .describedAs("N");

  private final OptionSpec<String> optionQueryLogRotationFrequency =
      parser
          .accepts(OPT_QUERY_LOG_ROTATION_FREQUENCY, "The interval for rotating query log files")
          .withRequiredArg()
          .ofType(String.class)
          .describedAs(RotationFrequencyConverter.valuePattern())
          .defaultsTo(RotationFrequencyConverter.RotationFrequency.HOURLY.value);

  private final OptionSpec<ZonedDateTime> optionQueryLogStart =
      parser
          .accepts(
              OPT_QUERY_LOG_START,
              "Inclusive start date for query logs to export, value will be truncated to hour")
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(ZonedParser.withDefaultPattern(DayOffset.START_OF_DAY))
          .describedAs("2001-01-15[ 00:00:00.[000]]");
  private final OptionSpec<ZonedDateTime> optionQueryLogEnd =
      parser
          .accepts(
              OPT_QUERY_LOG_END,
              "Exclusive end date for query logs to export, value will be truncated to hour")
          .withOptionalArg()
          .ofType(Date.class)
          .withValuesConvertedBy(ZonedParser.withDefaultPattern(DayOffset.END_OF_DAY))
          .describedAs("2001-01-15[ 00:00:00.[000]]");

  // This is intentionally NOT provided as a default value to the
  // optionQueryLogEnd OptionSpec,
  // because some callers
  // such as ZonedIntervalIterable want to be able to distinguish a user-specified
  // value from this
  // dumper-specified default.
  private final ZonedDateTime OPT_QUERY_LOG_END_DEFAULT = ZonedDateTime.now(ZoneOffset.UTC);

  private final OptionSpec<String> optionFlags =
      parser
          .accepts("test-flags", "UNDOCUMENTED: for internal testing only")
          .withRequiredArg()
          .ofType(String.class);

  private final OptionSpec<Void> optionDryrun =
      parser
          .acceptsAll(Arrays.asList("dry-run", "n"), "Show export actions without executing.")
          .forHelp();

  public static final String OPT_QUERY_LOG_ALTERNATES_DEPRECATION_MESSAGE =
      "The "
          + OPT_QUERY_LOG_ALTERNATES
          + " option is deprecated, please use -Dteradata-logs.query-log-table and"
          + " -Dteradata-logs.sql-log-table instead";
  private final OptionSpec<String> optionQueryLogAlternates =
      parser
          .accepts(
              OPT_QUERY_LOG_ALTERNATES,
              "pair of alternate query log tables to export (teradata-logs only), by default "
                  + "logTable=dbc.DBQLogTbl and queryTable=dbc.DBQLSQLTbl, if --assessment flag"
                  + " is enabled, then logTable=dbc.QryLogV and queryTable=dbc.DBQLSQLTbl. "
                  + OPT_QUERY_LOG_ALTERNATES_DEPRECATION_MESSAGE)
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
  private final OptionSpec<String> optionRedshiftIAMSessionToken =
      parser.accepts(OPT_IAM_SESSIONTOKEN).withRequiredArg();
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
  private final OptionSpec<String> optionHiveKerberosUrl =
      parser
          .accepts(
              OPT_HIVE_KERBEROS_URL,
              "Kerberos URL to use to authenticate Hive Thrift API. Please note that we don't"
                  + " accept Kerberos `REALM` in the URL. Please ensure that the tool runs in an"
                  + " environment where the default `REALM` is known and used. It's recommended to"
                  + " generate a Kerberos ticket with the same user before running the dumper. The"
                  + " tool will prompt for credentials if a ticket is not provided.")
          .withOptionalArg()
          .ofType(String.class)
          .describedAs("principal/host");

  // Ranger.
  private final OptionSpec<Integer> optionRangerPageSize =
      parser
          .accepts(OPT_RANGER_PAGE_SIZE, "Set the page size used to fetch Ranger entries.")
          .withRequiredArg()
          .ofType(Integer.class)
          .defaultsTo(OPT_RANGER_PAGE_SIZE_DEFAULT);

  private final OptionSpec<String> optionRangerScheme =
      parser
          .accepts(OPT_RANGER_SCHEME, "The uri scheme used to fetch Ranger entries.")
          .withRequiredArg()
          .ofType(String.class)
          .defaultsTo(OPT_RANGER_SCHEME_DEFAULT);

  private final OptionSpec<Void> optionRangerDisableTlsValidation =
      parser.accepts(
          OPT_RANGER_DISABLE_TLS_VALIDATION,
          "Disables TLS certificate validation. Set to accept self-signed certificates.");

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

  private final OptionSpec<Boolean> optionTelemetry =
      parser
          .accepts(OPT_TELEMETRY, "Allows dumper telemetry to be turned on/off")
          .withOptionalArg()
          .ofType(Boolean.class)
          .defaultsTo(true);

  public final OptionSpec<String> optionHadoopHdfsSiteXml =
      parser
          .accepts(
              OPT_HADOOP_HDFS_SITE_XML,
              "Path to Hadoop's hdfs-site.xml (when using Kerberos auth).")
          .withOptionalArg()
          .ofType(String.class)
          .defaultsTo(OPT_HADOOP_HDFS_SITE_XML_DEFAULT);

  public final OptionSpec<String> optionHadoopCoreSiteXml =
      parser
          .accepts(
              OPT_HADOOP_CORE_SITE_XML,
              "Path to Hadoop's core-site.xml (when using Kerberos auth).")
          .withOptionalArg()
          .ofType(String.class)
          .defaultsTo(OPT_HADOOP_CORE_SITE_XML_DEFAULT);

  // Kerberos keytab path
  private final OptionSpec<String> optionKerberosKeytabPath =
      parser
          .accepts(
              OPT_KERBEROS_KEYTAB_PATH,
              "Kerberos keytab file used by Hadoop connector. "
                  + "Requires option "
                  + OPT_KERBEROS_PRINCIPAL)
          .withRequiredArg()
          .ofType(String.class);

  // Kerberos principal
  private final OptionSpec<String> optionKerberosPrincipal =
      parser
          .accepts(
              OPT_KERBEROS_PRINCIPAL,
              "Kerberos principal used by Hadoop connector. It is usually\n"
                  + "a Service principal of the form: DUMPER/webserver.example.com@KERBEROS.REALM\n"
                  + "or a User principal of the form: USER@KERBEROS.REALM\n"
                  + "This option is required by "
                  + OPT_KERBEROS_KEYTAB_PATH)
          .requiredIf(optionKerberosKeytabPath)
          .withRequiredArg()
          .ofType(String.class);

  private final OptionSpec<String> optionHadoopRpcProtection =
      parser
          .accepts(
              OPT_HADOOP_RPC_PROTECTION,
              "Protection of Hadoop rpc calls (when using Kerberos auth) "
                  + "Options are: authentication, privacy, integrity.")
          .withRequiredArg()
          .ofType(String.class);

  private final OptionSpec<String> optionHdfsPrincipalPrefix =
      parser
          .accepts(
              OPT_HDFS_PRINCIPAL_PREFIX, "HDFS node(s) principal prefix (when using Kerberos auth)")
          .withRequiredArg()
          .ofType(String.class)
          .defaultsTo(OPT_HDFS_PRINCIPAL_PREFIX_DEFAULT);

  private final OptionSpec<String> optionHdfsScanRootPath =
      parser
          .accepts(OPT_HDFS_SCAN_ROOT_PATH, "HDFS root path to be scanned recursively.")
          .withRequiredArg()
          .ofType(String.class)
          .defaultsTo(OPT_HDFS_SCAN_ROOT_PATH_DEFAULT);

  public final OptionSpec<Void> optionKerberosAuthForHadoop =
      parser
          .accepts(OPT_KERBEROS_AUTH_FOR_HADOOP, "Use Kerberos auth for Hadoop.")
          .requiredIf(
              optionKerberosKeytabPath,
              optionKerberosPrincipal,
              optionHadoopRpcProtection,
              optionHdfsPrincipalPrefix);

  // Cloudera connector
  private final OptionSpec<String> optionYarnApplicationTypes =
      parser
          .accepts(
              OPT_YARN_APPLICATION_TYPES,
              "Dump Hadoop jobs by specific YARN application types. "
                  + "Has to be comma separated. For example: SPARK,MAPREDUCE,TEZ")
          .withOptionalArg()
          .ofType(String.class)
          .defaultsTo("");
  private final OptionSpec<Integer> optionPaginationPageSize =
      parser
          .accepts(OPT_PAGINATION_PAGE_SIZE, "Set page size for API requests.")
          .withOptionalArg()
          .ofType(Integer.class)
          .defaultsTo(1000);

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

  private ConnectorProperties connectorProperties;

  private final PasswordReader passwordReader = new PasswordReader();

  public ConnectorArguments(@Nonnull String... args) throws IOException {
    super(args);
  }

  @Override
  protected void printHelpOn(@Nonnull PrintStream out, OptionSet o) throws IOException {
    out.append(HELP_INFO);
    super.printHelpOn(out, o);

    ConnectorRepository repository = ConnectorRepository.getInstance();
    // if --connector <valid-connection> provided, print only that
    if (o.has(connectorNameOption)) {
      String helpOnConnector = o.valueOf(connectorNameOption);
      Connector connector = repository.getByName(helpOnConnector);
      if (connector != null) {
        out.append("\nSelected connector:\n");
        printConnectorHelp(out, connector);
        return;
      }
    }
    out.append("\nAvailable connectors:\n");
    for (Connector connector : repository.getAllConnectors()) {
      printConnectorHelp(out, connector);
    }
  }

  private static void printConnectorHelp(@Nonnull Appendable out, @Nonnull Connector connector)
      throws IOException {
    connector.printHelp(out);
    ConnectorProperties.printHelp(out, connector);
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

  public boolean hasUri() {
    return has(optionUri);
  }

  @Nonnull
  public String getHostOrDefault() {
    return getHost(OPT_HOST_DEFAULT);
  }

  @CheckForNull
  public String getHost() {
    return getOptions().valueOf(optionHost);
  }

  @Nonnull
  public String getHost(@Nonnull String defaultHost) {
    return firstNonNull(getHost(), defaultHost);
  }

  @CheckForNull
  public Integer getPort() {
    return getOptions().valueOf(optionPort);
  }

  @Nonnegative
  public int getPort(@Nonnegative int defaultPort) {
    Integer customPort = getPort();
    if (customPort != null) {
      return customPort.intValue();
    }
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
    if (in == null || in.isEmpty()) {
      return Predicates.alwaysTrue();
    }
    return Predicates.in(new HashSet<>(in));
  }

  @Nonnull
  public ImmutableList<String> getDatabases() {
    return getOptions().valuesOf(optionDatabase).stream()
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .collect(toImmutableList());
  }

  public boolean isDatabasesProvided() {
    return has(optionDatabase);
  }

  @CheckForNull
  public String getSchema() {
    return getOptions().valueOf(optionSchema);
  }

  @Nonnull
  public Predicate<String> getDatabasePredicate() {
    return toPredicate(getDatabases());
  }

  /** Returns the name of the single database specified, if exactly one database was specified. */
  // This can be used to generate an output filename, but it makes 1 be a special
  // case
  // that I find a little uncomfortable from the Unix philosophy:
  // "Sometimes the output filename is different" is hard to automate around.
  @CheckForNull
  public String getDatabaseSingleName() {
    List<String> databases = getDatabases();
    if (databases.size() == 1) {
      return databases.get(0);
    } else {
      return null;
    }
  }

  @Nonnull
  public List<String> getSchemata() {
    return getOptions().valuesOf(optionSchema);
  }

  public boolean isAssessment() {
    return getOptions().has(optionAssessment);
  }

  @Nonnull
  public Predicate<String> getSchemaPredicate() {
    return toPredicate(getSchemata());
  }

  @CheckForNull
  public String getUser() {
    return getOptions().valueOf(optionUser);
  }

  @Nonnull
  public String getUserOrFail() {
    String user = getOptions().valueOf(optionUser);
    if (user == null) {
      throw new MetadataDumperUsageException(
          "Required username was not provided. Please use the '--"
              + OPT_USER
              + "' flag to provide the username.");
    }
    return user;
  }

  /**
   * Get a password depending on the --password flag.
   *
   * @return An empty optional if the --password flag is not provided. Otherwise, an optional
   *     containing the result of getPasswordOrPrompt()
   */
  @Nonnull
  public Optional<String> getPasswordIfFlagProvided() {
    if (getOptions().has(optionPass)) {
      return Optional.of(getPasswordOrPrompt());
    } else {
      return Optional.empty();
    }
  }

  @Nonnull
  public String getPasswordOrPrompt() {
    String password = getOptions().valueOf(optionPass);
    if (password != null) {
      return password;
    } else {
      return passwordReader.getOrPrompt();
    }
  }

  public boolean isPasswordFlagProvided() {
    return has(optionPass);
  }

  public boolean isPrivateKeyFileProvided() {
    return has(optionPrivateKeyFile);
  }

  @CheckForNull
  public String getPrivateKeyFile() {
    return getOptions().valueOf(optionPrivateKeyFile);
  }

  @CheckForNull
  public String getPrivateKeyPassword() {
    return getOptions().valueOf(optionPrivateKeyPassword);
  }

  @CheckForNull
  public String getCluster() {
    return getOptions().valueOf(optionCluster);
  }

  @CheckForNull
  public String getRole() {
    return getOptions().valueOf(optionRole);
  }

  @Nonnull
  public List<String> getConfiguration() {
    return getOptions().valuesOf(optionConfiguration);
  }

  public Optional<String> getOutputFile() {
    if (!getOptions().has(optionOutput)) {
      return Optional.empty();
    }
    String file = getOptions().valueOf(optionOutput);
    if (file == null || file.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(file);
    }
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

  public boolean hasQueryLogEarliestTimestamp() {
    return getOptions().valueOf(optionQueryLogEarliestTimestamp) != null;
  }

  @CheckForNull
  public Integer getQueryLogDays() {
    return getOptions().valueOf(optionQueryLogDays);
  }

  @CheckForNull
  public ZonedDateTime getStartDate() {
    return getOptions().valueOf(optionStartDate);
  }

  public ZonedDateTime getStartDate(ZonedDateTime defaultTime) {
    return firstNonNull(getStartDate(), defaultTime);
  }

  @CheckForNull
  public ZonedDateTime getEndDate() {
    return getOptions().valueOf(optionEndDate);
  }

  public Duration getQueryLogRotationFrequency() {
    return RotationFrequencyConverter.convert(
        getOptions().valueOf(optionQueryLogRotationFrequency));
  }

  private static class RotationFrequencyConverter {

    private enum RotationFrequency {
      HOURLY(HOURS, "hourly"),
      DAILY(DAYS, "daily");

      private final ChronoUnit chronoUnit;
      private final String value;

      RotationFrequency(ChronoUnit chronoUnit, String value) {
        this.chronoUnit = chronoUnit;
        this.value = value;
      }
    }

    private RotationFrequencyConverter() {}

    private static Duration convert(String value) {
      for (RotationFrequency frequency : RotationFrequency.values()) {
        if (frequency.value.equals(value)) {
          return frequency.chronoUnit.getDuration();
        }
      }
      throw new MetadataDumperUsageException(
          String.format("Not a valid rotation frequency '%s'.", value));
    }

    private static String valuePattern() {
      return stream(RotationFrequency.values()).map(unit -> unit.value).collect(joining(", "));
    }
  }

  @Nonnegative
  public int getQueryLogDays(@Nonnegative int defaultQueryLogDays) {
    Integer out = getQueryLogDays();
    if (out != null) {
      return out.intValue();
    }
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

  @Nonnull
  public List<String> getQueryLogAlternates() {
    return getOptions().valuesOf(optionQueryLogAlternates);
  }

  public boolean isTelemetryOn() {
    return getOptions().valueOf(optionTelemetry);
  }

  public boolean isTestFlag(char c) {
    String flags = getOptions().valueOf(optionFlags);
    if (flags == null) {
      return false;
    }
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
  public String getIamSessionToken() {
    return getOptions().valueOf(optionRedshiftIAMSessionToken);
  }

  @CheckForNull
  public String getIAMProfile() {
    return getOptions().valueOf(optionRedshiftIAMProfile);
  }

  public int getRangerPageSizeDefault() {
    return getOptions().valueOf(optionRangerPageSize);
  }

  public String getRangerScheme() {
    return getOptions().valueOf(optionRangerScheme);
  }

  public boolean hasRangerIgnoreTlsValidation() {
    return getOptions().has(optionRangerDisableTlsValidation);
  }

  public int getThreadPoolSize() {
    return getOptions().valueOf(optionThreadPoolSize);
  }

  public boolean useKerberosAuthForHadoop() {
    return getOptions().has(optionKerberosAuthForHadoop);
  }

  public String getHadoopCoreSiteXml() {
    return getOptions().valueOf(optionHadoopCoreSiteXml);
  }

  public String getHadoopHdfsSiteXml() {
    return getOptions().valueOf(optionHadoopHdfsSiteXml);
  }

  public String getKerberosPrincipal() {
    return getOptions().valueOf(optionKerberosPrincipal);
  }

  public String getKerberosKeytabPath() {
    return getOptions().valueOf(optionKerberosKeytabPath);
  }

  @CheckForNull
  public String getHadoopRpcProtection() {
    return getOptions().valueOf(optionHadoopRpcProtection);
  }

  public String getHdfsPrincipalPrefix() {
    return getOptions().valueOf(optionHdfsPrincipalPrefix);
  }

  public String getHdfsScanRootPath() {
    return getOptions().valueOf(optionHdfsScanRootPath);
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

  @CheckForNull
  public String getHiveKerberosUrl() {
    return getOptions().valueOf(optionHiveKerberosUrl);
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
    return getConnectorProperties().get(property);
  }

  public int getPaginationPageSize() {
    return getOptions().valueOf(optionPaginationPageSize);
  }

  public List<String> getYarnApplicationTypes() {
    String yarnAppTypesLine = getOptions().valueOf(optionYarnApplicationTypes);
    return ImmutableList.copyOf(
        stream(yarnAppTypesLine.split(","))
            .filter(item -> item.trim().length() > 0)
            .collect(Collectors.toList()));
  }

  /** Checks if the property was specified on the command-line. */
  public boolean isDefinitionSpecified(@Nonnull ConnectorProperty property) {
    return getConnectorProperties().isSpecified(property);
  }

  public ConnectorProperties getConnectorProperties() {
    if (connectorProperties == null) {
      connectorProperties =
          new ConnectorProperties(getConnectorName(), getOptions().valuesOf(definitionOption));
    }
    return connectorProperties;
  }

  @Override
  @Nonnull
  public String toString() {
    // We do not include password here b/c as of this writing,
    // this string representation is logged out to file by ArgumentsTask.
    ToStringHelper toStringHelper =
        MoreObjects.toStringHelper(this)
            .add(OPT_CONNECTOR, getConnectorName())
            .add(OPT_DRIVER, getDriverPaths())
            .add(OPT_HOST, getHost())
            .add(OPT_PORT, getPort())
            .add(OPT_WAREHOUSE, getWarehouse())
            .add(OPT_DATABASE, getDatabases())
            .add(OPT_USER, getUser())
            .add(OPT_CONFIG, getConfiguration())
            .add(OPT_OUTPUT, getOutputFile().orElse(null))
            .add(OPT_QUERY_LOG_EARLIEST_TIMESTAMP, getQueryLogEarliestTimestamp())
            .add(OPT_QUERY_LOG_DAYS, getQueryLogDays())
            .add(OPT_QUERY_LOG_START, getQueryLogStart())
            .add(OPT_QUERY_LOG_END, getQueryLogEnd())
            .add(OPT_QUERY_LOG_ALTERNATES, getQueryLogAlternates())
            .add(OPT_ASSESSMENT, isAssessment())
            .add(OPT_TELEMETRY, isTelemetryOn());
    getConnectorProperties().getDefinitionMap().forEach(toStringHelper::add);
    return toStringHelper.toString();
  }

  @CheckForNull
  public String getDefinitionOrDefault(ConnectorPropertyWithDefault property) {
    return getConnectorProperties().getOrDefault(property);
  }

  public boolean has(OptionSpec<?> option) {
    return getOptions().has(option);
  }
}
