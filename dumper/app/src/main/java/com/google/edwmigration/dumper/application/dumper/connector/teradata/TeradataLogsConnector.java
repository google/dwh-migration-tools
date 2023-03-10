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

import static com.google.edwmigration.dumper.application.dumper.connector.teradata.TeradataAssessmentLogsJdbcTask.ASSESSMENT_DEF_LOG_TABLE;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
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
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.TeradataLogsDumpFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        "L.WarningOnly",
        "L.StartTime"
    };
    private static final Logger LOG = LoggerFactory.getLogger(TeradataLogsConnector.class);

    public TeradataLogsConnector() {
        super("teradata-logs");
    }

    // to proxy for Terdata14LogsConnector
    protected TeradataLogsConnector(@Nonnull String name) {
        super(name);
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
            conditions.add("L.StartTime >= " + arguments.getQueryLogEarliestTimestamp());

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

    /** This is shared between all instances of TeradataLogsJdbcTask. */
    /* pp */ static class SharedState {

        /**
         * Whether a particular expression is valid against the particular target Teradata version.
         * This is a concurrent Map of immutable objects, so is threadsafe overall.
         */
        /* pp */ final ConcurrentMap<String, Boolean> expressionValidity = new ConcurrentHashMap<>();
    }
}
