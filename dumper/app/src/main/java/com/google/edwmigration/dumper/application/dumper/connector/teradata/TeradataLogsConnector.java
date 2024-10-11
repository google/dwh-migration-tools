/*
 * Copyright 2022-2024 Google LLC
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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
import com.google.edwmigration.dumper.application.dumper.connector.IntervalExpander;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.utils.PropertyParser;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author matt
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version >=15.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
@RespectsArgumentAssessment
public class TeradataLogsConnector extends AbstractTeradataConnector
    implements LogsConnector, TeradataLogsDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);
  static final String ASSESSMENT_DEF_LOG_TABLE = "dbc.QryLogV";

  /** The length of the VARCHAR column {@code DBQLSqlTbl.SQLTextInfo}. */
  static final int DBQLSQLTBL_SQLTEXTINFO_LENGTH = 31000;

  private static final Range<Long> MAX_SQL_LENGTH_RANGE =
      Range.closed(5000L, (long) DBQLSQLTBL_SQLTEXTINFO_LENGTH);

  public enum TeradataLogsConnectorProperty implements ConnectorPropertyWithDefault {
    /**
     * The default value is dynamically computed in the {@link QueryLogTableNamesResolver} class.
     */
    QUERY_LOGS_TABLE(
        "query-logs-table",
        "The name of the query logs table. Default without --assessment flag: "
            + DEF_LOG_TABLE
            + ", default with --assessment flag: "
            + ASSESSMENT_DEF_LOG_TABLE,
        /* defaultValue= */ null),

    SQL_LOGS_TABLE(
        "sql-logs-table", "The name of the query logs table containing SQL text.", DEF_SQL_TABLE),
    UTILITY_LOGS_TABLE(
        "utility-logs-table", "The name of the utility logs table.", "dbc.DBQLUtilityTbl"),
    RES_USAGE_SCPU_TABLE(
        "res-usage-scpu-table",
        "The name of the SCPU resource usage logs table.",
        "dbc.ResUsageScpu"),
    RES_USAGE_SPMA_TABLE(
        "res-usage-spma-table",
        "The name of the SPMA resource usage logs table.",
        "dbc.ResUsageSpma"),

    /**
     * By default, the DATE column is not used in the join condition for the query tables, so the
     * default value is not provided.
     */
    LOG_DATE_COLUMN(
        "log-date-column",
        "The name of the column of type DATE to include in the WHERE clause and JOIN condition"
            + " when extracting query log tables. The column must exist in both tables."
            + " See -D"
            + QUERY_LOGS_TABLE.getName()
            + " and -D"
            + SQL_LOGS_TABLE.getName()
            + " for"
            + " the table names.",
        /* defaultValue= */ null),

    /**
     * By default, the SQL text splitting is not activated, so the default value is not provided.
     */
    MAX_SQL_LENGTH(
        "max-sql-length",
        "Max length of the DBQLSqlTbl.SqlTextInfo column."
            + " Text that is longer than the defined limit will be split into multiple rows."
            + " Example: 10000. Allowed range: "
            + MAX_SQL_LENGTH_RANGE
            + ".",
        /* defaultValue= */ null),
    TMODE(CommonTeradataConnectorProperty.TMODE);

    private final String name;
    private final String description;
    private final String defaultValue;

    TeradataLogsConnectorProperty(String name, String description, String defaultValue) {
      this.name = "teradata-logs." + name;
      this.description = description;
      this.defaultValue = defaultValue;
    }

    TeradataLogsConnectorProperty(CommonTeradataConnectorProperty connectorProperty) {
      this.name = connectorProperty.getName();
      this.description = connectorProperty.getDescription();
      this.defaultValue = null;
    }

    @Nonnull
    public String getName() {
      return name;
    }

    @Nonnull
    public String getDescription() {
      return description + (defaultValue != null ? " Default: " + defaultValue : "");
    }

    public String getDefaultValue() {
      return defaultValue;
    }
  }

  private static final ImmutableMap<TeradataLogsConnectorProperty, String>
      TIME_SERIES_PROPERTY_TO_FILENAME_PREFIX_MAP =
          ImmutableMap.of(
              TeradataLogsConnectorProperty.RES_USAGE_SCPU_TABLE, "dbc.ResUsageScpu_",
              TeradataLogsConnectorProperty.RES_USAGE_SPMA_TABLE, "dbc.ResUsageSpma_");

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return TeradataLogsConnectorProperty.class;
  }

  public TeradataLogsConnector() {
    super("teradata-logs");
  }

  private ImmutableList<TeradataJdbcSelectTask> createTimeSeriesTasks(
      ZonedInterval interval, @Nonnull ConnectorArguments arguments) {
    return TIME_SERIES_PROPERTY_TO_FILENAME_PREFIX_MAP.keySet().stream()
        .map(
            property ->
                new TeradataJdbcSelectTask(
                    createFilename(
                        TIME_SERIES_PROPERTY_TO_FILENAME_PREFIX_MAP.get(property), interval),
                    TaskCategory.OPTIONAL,
                    String.format(
                        "SELECT %%s FROM %s WHERE TheTimestamp >= %s AND TheTimestamp < %s",
                        arguments.getDefinitionOrDefault(property),
                        interval.getStart().toEpochSecond(),
                        interval.getEndExclusive().toEpochSecond())))
        .collect(toImmutableList());
  }

  private String createFilename(String zipEntryPrefix, ZonedInterval interval) {
    return zipEntryPrefix
        + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
        + ".csv";
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    boolean isAssessment = arguments.isAssessment();
    QueryLogTableNames tableNames = QueryLogTableNamesResolver.resolve(arguments);
    LOG.info(
        "Extracting query logs from tables: '{}' and '{}'",
        tableNames.queryLogsTableName(),
        tableNames.sqlLogsTableName());

    out.add(
        new TeradataTablesValidatorTask(
            tableNames.queryLogsTableName(), tableNames.sqlLogsTableName()));

    ImmutableSet.Builder<String> conditionsBuilder = ImmutableSet.builder();
    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp())) {
      conditionsBuilder.add("L.StartTime >= " + arguments.getQueryLogEarliestTimestamp());
    }

    Duration rotationDuration = arguments.getQueryLogRotationFrequency();
    ZonedIntervalIterable intervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, rotationDuration, IntervalExpander.createBasedOnDuration(rotationDuration));
    LOG.info("Exporting query logs for '{}'", intervals);

    String logDateColumn = arguments.getDefinition(TeradataLogsConnectorProperty.LOG_DATE_COLUMN);
    if (isAssessment
        && tableNames.usingAtLeastOneAlternate()
        && !arguments.isDefinitionSpecified(TeradataLogsConnectorProperty.LOG_DATE_COLUMN)) {
      LOG.info(
          "Extracting query logs from alternate tables without using the DATE column in the"
              + " JOIN condition. If both query logs tables ('{}' and '{}') contain a column of type"
              + " DATE in the Partitioned Primary Index, the extraction performance can be improved"
              + " by specifying the name of the column using the '-D{}' flag.",
          tableNames.queryLogsTableName(),
          tableNames.sqlLogsTableName(),
          TeradataLogsConnectorProperty.LOG_DATE_COLUMN.getName());
    }
    OptionalLong maxSqlLength =
        PropertyParser.parseNumber(
            arguments, TeradataLogsConnectorProperty.MAX_SQL_LENGTH, MAX_SQL_LENGTH_RANGE);
    if (!isAssessment) {
      conditionsBuilder.add("L.UserName <> 'DBC'");
    }

    SharedState queryLogsState = new SharedState();
    SharedState utilityLogsState = new SharedState();
    ImmutableSet<String> conditions = conditionsBuilder.build();

    if (isAssessment) {
      String utilityLogsTable =
          arguments.getDefinitionOrDefault(TeradataLogsConnectorProperty.UTILITY_LOGS_TABLE);

      addFailFastValidationStepForAssesment(out, arguments, utilityLogsTable);

      for (ZonedInterval interval : intervals) {
        String file = createFilename(ZIP_ENTRY_PREFIX, interval);
        List<String> orderBy = Arrays.asList("ST.QueryID", "ST.SQLRowNo");
        out.add(
            new TeradataAssessmentLogsJdbcTask(
                    file,
                    queryLogsState,
                    tableNames,
                    conditions,
                    interval,
                    logDateColumn,
                    maxSqlLength,
                    orderBy)
                .withHeaderClass(HeaderForAssessment.class));
        out.addAll(createTimeSeriesTasks(interval, arguments));
        out.add(
            new TeradataUtilityLogsJdbcTask(
                createFilename("utility_logs_", interval),
                utilityLogsState,
                utilityLogsTable,
                interval));
      }

      out.add(
          new TeradataQueryLogsJdbcTask(
              "queryLogsFirstAndLastEntry.csv",
              tableNames.queryLogsTableName(),
              intervals.getStart(),
              intervals.getEnd()));
    } else {
      for (ZonedInterval interval : intervals) {
        String file = createFilename(ZIP_ENTRY_PREFIX, interval);
        out.add(
            new TeradataLogsJdbcTask(file, queryLogsState, tableNames, conditions, interval)
                .withHeaderClass(Header.class));
      }
    }
  }

  private void addFailFastValidationStepForAssesment(
      List<? super Task<?>> out, ConnectorArguments arguments, String utilityLogsTable) {
    List<String> optionalTablesToCheck = new ArrayList<>();
    optionalTablesToCheck.add(utilityLogsTable);

    if (arguments.isDefinitionSpecified(TeradataLogsConnectorProperty.RES_USAGE_SCPU_TABLE)) {
      optionalTablesToCheck.add(
          arguments.getDefinition(TeradataLogsConnectorProperty.RES_USAGE_SCPU_TABLE));
    }
    if (arguments.isDefinitionSpecified(TeradataLogsConnectorProperty.RES_USAGE_SPMA_TABLE)) {
      optionalTablesToCheck.add(
          arguments.getDefinition(TeradataLogsConnectorProperty.RES_USAGE_SPMA_TABLE));
    }

    out.add(new TeradataTablesValidatorTask(optionalTablesToCheck.toArray(new String[] {})));
  }
}
