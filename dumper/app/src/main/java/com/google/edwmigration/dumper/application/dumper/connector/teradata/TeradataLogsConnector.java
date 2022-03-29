/*
 * Copyright 2022 Google LLC
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
import com.google.common.base.Strings;
import com.google.common.io.ByteSink;
import com.google.errorprone.annotations.ForOverride;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 *
 * @author matt
 *
 * TODO :Make a base class, and derive TeradataLogs and TeradataLogs14 from it
 */
@AutoService({Connector.class, LogsConnector.class})
@Description("Dumps logs from Teradata version >=15.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
public class TeradataLogsConnector extends AbstractTeradataConnector implements LogsConnector, TeradataLogsDumpFormat {

    private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);

    protected static final DateTimeFormatter SQL_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);
    @VisibleForTesting
    /* pp */ static final String DEF_LOG_TABLE = "dbc.DBQLogTbl";
    @VisibleForTesting
    /* pp */ static final String DEF_QUERY_TABLE = "dbc.DBQLSQLTbl";

    // Docref: https://docs.teradata.com/reader/wada1XMYPkZVTqPKz2CNaw/F7f64mU9~e4s03UAdorEHw
    // According to one customer, the attributes "SQLTextInfo", "LastRespTime",
    // "RequestMode", and "Statements" are all absent from DBQLogTbl in version 14.10.07.10,
    // and possibly others, hence our need to replace any missing projection attributes with NULLs.
    // MUST match TeradataLogsDumpFormat.Header
    @VisibleForTesting
    static final String[] EXPRESSIONS = new String[]{
        "ST.QueryID",
        "ST.SQLRowNo",
        "ST.SQLTextInfo",
        "L.UserName",
        "L.CollectTimeStamp",
        "L.StatementType",
        "L.AppID",
        "L.DefaultDatabase",
        "L.ErrorCode",
        "L.ErrorText",
        "L.FirstRespTime",
        "L.LastRespTime",
        "L.NumResultRows",
        "L.QueryText",
        "L.ReqPhysIO",
        "L.ReqPhysIOKB",
        "L.RequestMode",
        "L.SessionID",
        "L.SessionWDID",
        "L.Statements",
        "L.TotalIOCount",
        "L.WarningOnly"
    };

    public static boolean isQueryTable(@Nonnull String expression) {
        return expression.startsWith("ST.");
    }

    public TeradataLogsConnector() {
        super("teradata-logs");
    }

    // to proxy for Terdata14LogsConnector
    protected TeradataLogsConnector(@Nonnull String name) {
        super(name);
    }

    /** This is shared between all instances of TeradataLogsJdbcTask. */
    protected static class SharedState {

        /**
         * Whether a particular expression is valid against the particular target Teradata version.
         * This is a concurrent Map of immutable objects, so is threadsafe overall.
         */
        private final ConcurrentMap<String, Boolean> expressionValidity = new ConcurrentHashMap<>();
    }

    protected static class TeradataLogsJdbcTask extends AbstractJdbcTask<Void> {

        @VisibleForTesting
        public static String EXPRESSION_VALIDITY_QUERY = "SELECT TOP 1 %s FROM %s";

        protected final SharedState state;
        protected final String logTable;
        protected final String queryTable;
        protected final String condition;
        protected final ZonedInterval interval;

        public TeradataLogsJdbcTask(@Nonnull String targetPath, SharedState state, String logTable, String queryTable, String condition, ZonedInterval interval) {
            super(targetPath);
            this.state = Preconditions.checkNotNull(state, "SharedState was null.");
            this.logTable = logTable;
            this.queryTable = queryTable;
            this.condition = condition;
            this.interval = interval;
        }

        @Override
        protected Void doInConnection(TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection) throws SQLException {
            String sql = getSql(jdbcHandle);
            ResultSetExtractor<Void> rse = newCsvResultSetExtractor(sink, -1);
            return doSelect(connection, rse, sql);
        }

        @Nonnull
        private String getSql(@Nonnull JdbcHandle handle) {
            Function<String, Boolean> validator = expression -> isValid(handle.getJdbcTemplate(), expression);
            Predicate<String> predicate = expression -> state.expressionValidity.computeIfAbsent(expression, validator);
            String sql = getSql(predicate);
            // LOG.debug("SQL is " + sql);
            return sql;
        }

        /**
         * For each potential expression in EXPRESSIONS, work out whether this Teradata accepts it,
         * and if so, use it as part of the eventual query.
         *
         * @param predicate A predicate to compute whether a given expression is legal.
         * @return A SQL query containing every legal expression from EXPRESSIONS.
         */
        @ForOverride
        @Nonnull
        /* pp */ String getSql(@Nonnull Predicate<? super String> predicate) {
            StringBuilder buf = new StringBuilder("SELECT ");

            String separator = "";
            boolean queryTableIncluded = false;
            for (String expression : EXPRESSIONS) {
                buf.append(separator);
                if (predicate.test(expression)) {
                    buf.append(expression);
                    if (isQueryTable(expression))
                        queryTableIncluded = true;
                } else {
                    buf.append("NULL");
                }
                separator = ", ";
            }

            buf.append(" FROM ").append(logTable).append(" L");

            if (queryTableIncluded) {
                // "QueryID is a system-wide unique field; you can use QueryID
                // to join DBQL tables ... without needing ProcID as an additional join field."
                // (https://docs.teradata.com/reader/B7Lgdw6r3719WUyiCSJcgw/YIKoBz~QQgv2Aw5dF339kA)
                buf.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.QueryID=ST.QueryID");

                // Notwithstanding the above: could this offer improved perf due to use of indices?:
                // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLSqlTbl.htm
                // http://elsasoft.com/samples/teradata/Teradata.127.0.0.1.DBC/table_DBQLogTbl.htm
                // Testing on a PostgreSQL-backed-db indicates this is actually slightly slower; maybe on-site actual TD will perform better?
                // buf.append(" LEFT OUTER JOIN ").append(queryTable).append(" ST ON L.ProcID=ST.ProcID AND L.CollectTimeStamp=ST.CollectTimeStamp AND L.QueryID=ST.QueryID");
            }

            buf.append(String.format(" WHERE L.UserName <> 'DBC'\n" // not useful (according to a customer)
                    + "AND L.ErrorCode=0\n" // errors are not useful to us
                    + "AND L.CollectTimeStamp >= CAST('%s' AS TIMESTAMP)\n"
                    + "AND L.CollectTimeStamp < CAST('%s' AS TIMESTAMP)\n",
                    SQL_FORMAT.format(interval.getStart()), SQL_FORMAT.format(interval.getEndExclusive())));

            if (!Strings.isNullOrEmpty(condition))
                buf.append(" AND ").append(condition);

            return buf.toString().replace('\n', ' ');
        }

        /** Runs a test query to check whether a given projection expression is legal on this Teradata instance. */
        @Nonnull
        private Boolean isValid(@Nonnull JdbcTemplate template, @Nonnull String expression) {
            String table = isQueryTable(expression) ? queryTable + " ST" : logTable + " L";
            String sql = String.format(EXPRESSION_VALIDITY_QUERY, expression, table);
            LOG.info("Checking legality of projection expression '{}' using query: {}", expression, sql);
            try {
                template.query(sql, rs -> {
                });
                return Boolean.TRUE;
            } catch (DataAccessException e) {
                LOG.info("Attribute '{}' is absent, will use NULL in projection: {}", expression, e.getMessage());
                return Boolean.FALSE;
            }
        }

        @Override
        public String toString() {
            return getSql(Predicates.alwaysTrue());
        }
    }

    @Override
    public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) throws MetadataDumperUsageException {
        out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
        out.add(new FormatTask(FORMAT_NAME));

        String logTable = DEF_LOG_TABLE;
        String queryTable = DEF_QUERY_TABLE;
        List<String> alternates = arguments.getQueryLogAlternates();
        if (!alternates.isEmpty()) {
            if (alternates.size() != 2)
                throw new MetadataDumperUsageException("Alternate query log tables must be given as a pair; you specified: " + alternates);
            logTable = alternates.get(0);
            queryTable = alternates.get(1);
        }

        // if the user specifies an earliest start time there will be extraneous empty dump files
        // because we always iterate over the full 7 trailing days; maybe it's worth
        // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
        // to parse and return an ISO instant, not a database-server-specific format.
        String condition;
        if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
            condition = "L.CollectTimeStamp >= " + arguments.getQueryLogEarliestTimestamp();
        else
            condition = null;

        // Beware of Teradata SQLSTATE HY000. See issue #4126.
        // Most likely caused by some operation (equality?) being performed on a datum which is too long for a varchar.
        ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
        LOG.info("Exporting query log for " + intervals);
        SharedState state = new SharedState();
        for (ZonedInterval interval : intervals) {
            String file = ZIP_ENTRY_PREFIX + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC()) + ".csv";
            out.add(new TeradataLogsJdbcTask(file, state, logTable, queryTable, condition, interval).withHeaderClass(Header.class));
        }
    }
}
