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
package com.google.edwmigration.dumper.application.dumper.connector.teradata;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/** */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version <=14.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
@RespectsArgumentAssessment
public class Teradata14LogsConnector extends AbstractTeradataConnector
    implements LogsConnector, TeradataLogsDumpFormat {

  private static final Logger logger = LoggerFactory.getLogger(Teradata14LogsConnector.class);

  @VisibleForTesting
  static final List<String> EXPRESSIONS_LSQL_TBL =
      enumNames("ST.", TeradataLogsDumpFormat.HeaderLSql.class);

  @VisibleForTesting
  static final List<String> EXPRESSIONS_LOG_TBL =
      enumNames("L.", TeradataLogsDumpFormat.HeaderLog.class);

  private static List<String> enumNames(String prefix, Class<? extends Enum<?>> en) {
    Enum<?> v[] = en.getEnumConstants();
    List<String> ret = new ArrayList<>(v.length);
    for (Enum<?> h : v) ret.add(prefix + h.name());
    return ret;
  }

  public Teradata14LogsConnector() {
    super("teradata14-logs");
  }

  private abstract static class Teradata14LogsJdbcTask extends AbstractJdbcTask<Summary> {

    protected static String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";

    protected final SharedState state;
    protected final String logTable;
    protected final String queryTable;
    protected final List<String> conditions;
    protected final ZonedInterval interval;
    protected final List<String> orderBy;

    public Teradata14LogsJdbcTask(
        @Nonnull String targetPath,
        SharedState state,
        String logTable,
        String queryTable,
        List<String> conditions,
        ZonedInterval interval) {
      this(targetPath, state, logTable, queryTable, conditions, interval, Collections.emptyList());
    }

    protected Teradata14LogsJdbcTask(
        @Nonnull String targetPath,
        SharedState state,
        String logTable,
        String queryTable,
        List<String> conditions,
        ZonedInterval interval,
        List<String> orderBy) {
      super(targetPath);
      this.state = Preconditions.checkNotNull(state, "SharedState was null.");
      this.logTable = logTable;
      this.queryTable = queryTable;
      this.conditions = conditions;
      this.interval = interval;
      this.orderBy = orderBy;
    }

    @Override
    protected Summary doInConnection(
        @Nonnull TaskRunContext context,
        @Nonnull JdbcHandle jdbcHandle,
        @Nonnull ByteSink sink,
        @Nonnull Connection connection)
        throws SQLException {
      String sql = getSql(jdbcHandle);
      ResultSetExtractor<Summary> rse = newCsvResultSetExtractor(sink);
      return doSelect(connection, withInterval(rse, interval), sql);
    }

    @Nonnull
    private String getSql(@Nonnull JdbcHandle handle) {
      Function<String, Boolean> validator =
          expression -> isValid(handle.getJdbcTemplate(), expression);
      Predicate<String> predicate =
          expression -> state.expressionValidity.computeIfAbsent(expression, validator);
      return getSql(predicate);
    }

    @Nonnull
    protected abstract String getSql(@Nonnull Predicate<String> predicate);

    /**
     * Runs a test query to check whether a given projection expression is legal on this Teradata
     * instance.
     */
    @Nonnull
    private Boolean isValid(@Nonnull JdbcTemplate template, @Nonnull String expression) {
      String table = isQueryTable(expression) ? queryTable + " ST" : logTable + " L";
      String sql = String.format(EXPRESSION_VALIDITY_QUERY, expression, table);
      logger.info(
          "Checking legality of projection expression '{}' using query: {}", expression, sql);
      try {
        template.query(sql, rs -> {});
        return Boolean.TRUE;
      } catch (DataAccessException e) {
        logger.info(
            "Attribute '{}' is absent, will use NULL in projection: {}",
            expression,
            e.getMessage());
        return Boolean.FALSE;
      }
    }

    @Override
    public String describeSourceData() {
      return "from " + getSql(Predicates.alwaysTrue());
    }
  }

  private static class LSqlQueryFactory extends Teradata14LogsJdbcTask {

    public LSqlQueryFactory(
        String targetPath,
        SharedState state,
        String logTable,
        String queryTable,
        List<String> conditions,
        ZonedInterval interval) {
      super(targetPath, state, logTable, queryTable, conditions, interval);
    }

    @Override
    @Nonnull
    protected String getSql(@Nonnull Predicate<String> predicate) {
      StringBuilder buf = new StringBuilder("SELECT ");

      String separator = "";
      for (String expression : EXPRESSIONS_LSQL_TBL) {
        buf.append(separator);
        if (predicate.test(expression)) {
          buf.append(expression);
        } else {
          buf.append("NULL");
        }
        separator = ", ";
      }

      buf.append(" FROM ").append(queryTable).append(" ST ");

      buf.append(
          String.format(
              "WHERE ST.CollectTimeStamp >= CAST('%s' AS TIMESTAMP)\n"
                  + "AND ST.CollectTimeStamp < CAST('%s' AS TIMESTAMP)\n",
              SQL_FORMAT.format(interval.getStart()),
              SQL_FORMAT.format(interval.getEndExclusive())));

      for (String condition : conditions) {
        buf.append(" AND ").append(condition);
      }

      return buf.toString().replace('\n', ' ');
    }
  }

  private static class LogQueryFactory extends Teradata14LogsJdbcTask {

    public LogQueryFactory(
        String targetPath,
        SharedState state,
        String logTable,
        String queryTable,
        List<String> conditions,
        ZonedInterval interval) {
      super(targetPath, state, logTable, queryTable, conditions, interval);
    }

    @Override
    @Nonnull
    protected String getSql(@Nonnull Predicate<String> predicate) {
      StringBuilder buf = new StringBuilder("SELECT ");

      String separator = "";
      for (String expression : EXPRESSIONS_LOG_TBL) {
        buf.append(separator);
        if (predicate.test(expression)) {
          buf.append(expression);
        } else {
          buf.append("NULL");
        }
        separator = ", ";
      }

      buf.append(" FROM ").append(logTable).append(" L ");

      buf.append(
          String.format(
              "WHERE L.StartTime >= CAST('%s' AS TIMESTAMP)\n"
                  + "AND L.StartTime < CAST('%s' AS TIMESTAMP)\n",
              SQL_FORMAT.format(interval.getStart()),
              SQL_FORMAT.format(interval.getEndExclusive())));

      for (String condition : conditions) {
        buf.append(" AND ").append(condition);
      }

      return buf.toString().replace('\n', ' ');
    }
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    String logTable = DEF_LOG_TABLE;
    String sqlTable = DEF_SQL_TABLE;
    List<String> alternates = arguments.getQueryLogAlternates();
    if (!alternates.isEmpty()) {
      if (alternates.size() != 2)
        throw new MetadataDumperUsageException(
            "Alternate query log tables must be given as a pair; you specified: " + alternates);
      logTable = alternates.get(0);
      sqlTable = alternates.get(1);
    }

    // if the user specifies an earliest start time there will be extraneous empty dump files
    // because we always iterate over the full 7 trailing days; maybe it's worth
    // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
    // to parse and return an ISO instant, not a database-server-specific format.
    List<String> lSqlConditions = new ArrayList<>();
    List<String> logConditions = new ArrayList<>();
    if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp())) {
      lSqlConditions.add("ST.CollectTimeStamp >= " + arguments.getQueryLogEarliestTimestamp());
      logConditions.add("L.StartTime >= " + arguments.getQueryLogEarliestTimestamp());
    }

    // Beware of Teradata SQLSTATE HY000. See issue #4126.
    // Most likely caused by some operation (equality?) being performed on a datum which is too long
    // for a varchar.
    ZonedIntervalIterable intervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(arguments);
    logger.info("Exporting query log for " + intervals);
    SharedState state = new SharedState();
    for (ZonedInterval interval : intervals) {
      String LSqlfile =
          ZIP_ENTRY_PREFIX_LSQL
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      out.add(
          new LSqlQueryFactory(LSqlfile, state, logTable, sqlTable, lSqlConditions, interval)
              .withHeaderClass(TeradataLogsDumpFormat.HeaderLSql.class));

      String LOGfile =
          ZIP_ENTRY_PREFIX_LOG
              + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC())
              + ".csv";
      out.add(
          new LogQueryFactory(LOGfile, state, logTable, sqlTable, logConditions, interval)
              .withHeaderClass(TeradataLogsDumpFormat.HeaderLog.class));
    }
  }
}
