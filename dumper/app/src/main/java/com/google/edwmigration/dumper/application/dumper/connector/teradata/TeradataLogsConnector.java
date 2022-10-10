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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentAssessment;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.errorprone.annotations.ForOverride;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataLogsConnector.TeradataAssessmentLogsJdbcTask.ASSESSMENT_DEF_LOG_TABLE;

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
@RespectsArgumentAssessment
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
        "L.QueryID",
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
        protected final List<String> conditions;
        protected final ZonedInterval interval;
        protected final List<String> orderBy;

        public TeradataLogsJdbcTask(@Nonnull String targetPath, SharedState state, String logTable,
                                    String queryTable, List<String> conditions, ZonedInterval interval) {
            this(targetPath, state, logTable, queryTable, conditions, interval, Collections.emptyList());
        }

        protected TeradataLogsJdbcTask(@Nonnull String targetPath, SharedState state, String logTable,
                                    String queryTable, List<String> conditions, ZonedInterval interval, List<String> orderBy) {
            super(targetPath);
            this.state = Preconditions.checkNotNull(state, "SharedState was null.");
            this.logTable = logTable;
            this.queryTable = queryTable;
            this.conditions = conditions;
            this.interval = interval;
            this.orderBy = orderBy;
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
            return getSql(predicate, EXPRESSIONS);
        }
        /* pp */ String getSql(Predicate<? super String> predicate, String[] expressions) {
            StringBuilder buf = new StringBuilder("SELECT ");
            String separator = "";
            boolean queryTableIncluded = false;
            for (String expression : expressions) {
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

            buf.append(String.format(" WHERE L.ErrorCode=0\n"
                    + "AND L.CollectTimeStamp >= CAST('%s' AS TIMESTAMP)\n"
                    + "AND L.CollectTimeStamp < CAST('%s' AS TIMESTAMP)\n",
                    SQL_FORMAT.format(interval.getStart()), SQL_FORMAT.format(interval.getEndExclusive())));

            for (String condition : conditions) {
                buf.append(" AND ").append(condition);
            }

            if (!orderBy.isEmpty()) {
                buf.append(" ORDER BY ");
                Joiner.on(", ").appendTo(buf, orderBy);
            }
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

    protected static class TeradataAssessmentLogsJdbcTask extends TeradataLogsJdbcTask {
        /* pp */ static final String ASSESSMENT_DEF_LOG_TABLE = "dbc.QryLogV";
        static final String[] EXPRESSIONS_FOR_ASSESSMENT = new String[]{
                "L.QueryID",
                "ST.SQLRowNo",
                "ST.SQLTextInfo",
                "L.AbortFlag",
                "L.AcctString",
                "L.AcctStringDate",
                "L.AcctStringHour",
                "L.AcctStringTime",
                "L.AMPCPUTime",
                "L.AMPCPUTimeNorm",
                "L.AppID",
                "L.CacheFlag",
                "L.CalendarName",
                "L.CallNestingLevel",
                "L.CheckpointNum",
                "L.ClientAddr",
                "L.ClientID",
                "L.CollectTimeStamp AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"CollectTimeStamp\"",
                "L.CPUDecayLevel",
                "L.DataCollectAlg",
                "L.DBQLStatus",
                "L.DefaultDatabase",
                "L.DelayTime",
                "L.DisCPUTime",
                "L.DisCPUTimeNorm",
                "L.ErrorCode",
                "L.EstMaxRowCount",
                "L.EstMaxStepTime",
                "L.EstProcTime",
                "L.EstResultRows",
                "L.ExpandAcctString",
                "L.FirstRespTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"FirstRespTime\"",
                "L.FirstStepTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"FirstStepTime\"",
                "L.FlexThrottle",
                "L.ImpactSpool",
                "L.InternalRequestNum",
                "L.IODecayLevel",
                "L.IterationCount",
                "L.KeepFlag",
                "L.LastRespTime",
                "L.LockDelay",
                "L.LockLevel",
                "L.LogicalHostID",
                "L.LogonDateTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"LogonDateTime\"",
                "L.LogonSource",
                "L.LSN",
                "L.MaxAMPCPUTime",
                "L.MaxAMPCPUTimeNorm",
                "L.MaxAmpIO",
                "L.MaxCPUAmpNumber",
                "L.MaxCPUAmpNumberNorm",
                "L.MaxIOAmpNumber",
                "L.MaxNumMapAMPs",
                "L.MaxOneMBRowSize",
                "L.MaxStepMemory",
                "L.MaxStepsInPar",
                "L.MinAmpCPUTime",
                "L.MinAmpCPUTimeNorm",
                "L.MinAmpIO",
                "L.MinNumMapAMPs",
                "L.MinRespHoldTime",
                "L.NumFragments",
                "L.NumOfActiveAMPs",
                "L.NumRequestCtx",
                "L.NumResultOneMBRows",
                "L.NumResultRows",
                "L.NumSteps",
                "L.NumStepswPar",
                "L.ParamQuery",
                "L.ParserCPUTime",
                "L.ParserCPUTimeNorm",
                "L.ParserExpReq",
                "L.PersistentSpool",
                "L.ProcID",
                "L.ProfileID",
                "L.ProfileName",
                "L.ProxyRole",
                "L.ProxyUser",
                "L.ProxyUserID",
                "L.QueryBand",
                "L.QueryRedriven",
                "L.QueryText",
                "L.ReDriveKind",
                "L.RemoteQuery",
                "L.ReqIOKB",
                "L.ReqPhysIO",
                "L.ReqPhysIOKB",
                "L.RequestMode",
                "L.RequestNum",
                "L.SeqRespTime",
                "L.SessionID",
                "L.SessionTemporalQualifier",
                "L.SpoolUsage",
                "L.StartTime AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE AS \"StartTime\"",
                "L.StatementGroup",
                "L.Statements",
                "L.StatementType",
                "L.SysDefNumMapAMPs",
                "L.TacticalCPUException",
                "L.TacticalIOException",
                "L.TDWMEstMemUsage",
                "L.ThrottleBypassed",
                "L.TotalFirstRespTime",
                "L.TotalIOCount",
                "L.TotalServerByteCount",
                "L.TTGranularity",
                "L.TxnMode",
                "L.TxnUniq",
                "L.UnitySQL",
                "L.UnityTime",
                "L.UsedIota",
                "L.UserID",
                "L.UserName",
                "L.UtilityByteCount",
                "L.UtilityInfoAvailable",
                "L.UtilityRowCount",
                "L.VHLogicalIO",
                "L.VHLogicalIOKB",
                "L.VHPhysIO",
                "L.VHPhysIOKB",
                "L.WarningOnly",
                "L.WDName"
        };

        public TeradataAssessmentLogsJdbcTask(@Nonnull String targetPath, SharedState state, String logTable, String queryTable, List<String> conditions, ZonedInterval interval, List<String> orderBy) {
            super(targetPath, state, logTable, queryTable, conditions, interval, orderBy);
        }

        @Nonnull
        @Override
        String getSql(@Nonnull Predicate<? super String> predicate) {
            return getSql(predicate, EXPRESSIONS_FOR_ASSESSMENT);
        }
    }
    @Override
    public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) throws MetadataDumperUsageException {
        out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
        out.add(new FormatTask(FORMAT_NAME));

        boolean isAssessment = arguments.isAssessment();
        String logTable = isAssessment ? ASSESSMENT_DEF_LOG_TABLE : DEF_LOG_TABLE;
        String queryTable = DEF_QUERY_TABLE;
        List<String> alternates = arguments.getQueryLogAlternates();
        if (!alternates.isEmpty()) {
            if (alternates.size() != 2)
                throw new MetadataDumperUsageException("Alternate query log tables must be given as a pair; you specified: " + alternates);
            logTable = alternates.get(0);
            queryTable = alternates.get(1);
        }
        List<String> conditions = new ArrayList<>();
        // if the user specifies an earliest start time there will be extraneous empty dump files
        // because we always iterate over the full 7 trailing days; maybe it's worth
        // preventing that in the future. To do that, we should require getQueryLogEarliestTimestamp()
        // to parse and return an ISO instant, not a database-server-specific format.
        if (!StringUtils.isBlank(arguments.getQueryLogEarliestTimestamp()))
            conditions.add("L.CollectTimeStamp >= " + arguments.getQueryLogEarliestTimestamp());

        // Beware of Teradata SQLSTATE HY000. See issue #4126.
        // Most likely caused by some operation (equality?) being performed on a datum which is too long for a varchar.
        ZonedIntervalIterable intervals = ZonedIntervalIterable.forConnectorArguments(arguments);
        LOG.info("Exporting query log for " + intervals);
        SharedState state = new SharedState();
        for (ZonedInterval interval : intervals) {
            String file = ZIP_ENTRY_PREFIX + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(interval.getStartUTC()) + ".csv";
            if (isAssessment) {
                List<String> orderBy = Arrays.asList("ST.QueryID", "ST.SQLRowNo");
                out.add(new TeradataAssessmentLogsJdbcTask(file, state, logTable, queryTable, conditions, interval, orderBy)
                        .withHeaderClass(HeaderForAssessment.class));
            } else {
                conditions.add("L.UserName <> 'DBC'");
                out.add(new TeradataLogsJdbcTask(file, state, logTable, queryTable, conditions, interval)
                        .withHeaderClass(Header.class));
            }
        }
    }
}
