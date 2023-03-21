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
package com.google.edwmigration.dumper.application.dumper.connector.netezza;

import com.google.auto.service.AutoService;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDatabasePredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentDriverRequired;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentHostUnlessUrl;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentPassword;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUri;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentUser;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractJdbcConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.NetezzaMetadataDumpFormat;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * ./gradlew :compilerworks-application-dumper:installDist &&
 * ./compilerworks-application-dumper/build/install/compilerworks-application-dumper/bin/compilerworks-application-dumper
 * --connector netezza --user nz --host 192.168.199.128 --port 5480 --pass nz --driver
 * /path/to/nzjdbc3.jar
 *
 * @author miguel
 */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Netezza.")
@RespectsArgumentDriverRequired
@RespectsArgumentHostUnlessUrl
@RespectsInput(
    order = ConnectorArguments.OPT_PORT_ORDER,
    arg = ConnectorArguments.OPT_PORT,
    description = "The port of the server.",
    required = ConnectorArguments.OPT_REQUIRED_IF_NOT_URL,
    defaultValue = "" + NetezzaMetadataConnector.OPT_PORT_DEFAULT)
@RespectsArgumentUser
@RespectsArgumentPassword
@RespectsArgumentUri
@RespectsArgumentDatabasePredicate
public class NetezzaMetadataConnector extends AbstractJdbcConnector
    implements MetadataConnector, NetezzaMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(NetezzaMetadataConnector.class);

  public static final int OPT_PORT_DEFAULT = 5480;

  public NetezzaMetadataConnector() {
    super("netezza");
  }

  @SuppressWarnings("UnusedNestedClass")
  private static class NetezzaDatabaseListTask extends AbstractJdbcTask<List<String>> {

    private final String sql;

    public NetezzaDatabaseListTask(String targetPath, String sql) {
      super(targetPath);
      this.sql = Preconditions.checkNotNull(sql, "SQL was null.");
    }

    protected ResultSetExtractor<List<String>> newResultSetExtractor(
        @Nonnull ByteSink sink, @Nonnull JdbcHandle handle) {
      return new ResultSetExtractor<List<String>>() {
        @Override
        public List<String> extractData(ResultSet rs) throws SQLException, DataAccessException {
          CSVFormat format = newCsvFormat(rs);
          try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
              CSVPrinter printer = format.print(writer)) {
            List<String> out = new ArrayList<>();
            printer.printRecords(rs); // TODO: Append to out.
            return out;
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
      };
    }

    @Override
    protected List<String> doInConnection(
        TaskRunContext context, JdbcHandle jdbcHandle, ByteSink sink, Connection connection)
        throws SQLException {
      ResultSetExtractor<List<String>> rse = newResultSetExtractor(sink, jdbcHandle);
      return doSelect(connection, rse, sql);
    }

    @Override
    public List<String> run(TaskRunContext context) throws Exception {
      List<String> databaseNames = super.run(context);
      for (String databaseName :
          MoreObjects.firstNonNull(databaseNames, Collections.<String>emptyList())) {
        // TODO:
        // Construct a filter if --dbs was given on the command line.
        // For each db in the databaseNames [which passes the filter]
        // Add a set of tasks hauled up from the down below, and run them.
        // TODO:
        // Extract the databaseName from an appropriate SQL query in the ResultSetExtractor above.
        // TODO:
        // Use this task to list databases somewhere in the down-below addTasksTo.
        // context.runChildTask(...);
      }
      return databaseNames;
    }
  }

  // http://dwgeek.com/netezza-system-tables-views.html/
  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    Set<String> dbs = new HashSet<>(arguments.getDatabases());
    dbs.add("system");

    out.add(
        new JdbcSelectTask(
            "nz.version.csv",
            "SELECT 'netezza' AS dialect, "
                // CSVPrinter cannot handle the type:
                + "cast(version() as varchar(64)) as version, " // " + db +
                // ".admin.admin._v_system_info.SYSTEM_SOFTWARE_VERSION
                + "CURRENT_TIMESTAMP as export_time "));

    // NZ DB admin guide
    out.add(new JdbcSelectTask("nz.t_database.csv", "SELECT * FROM system.._t_database"));

    // See IBM Netezza System Administrators Guide, appendex, C3.
    // objid, Database, Owner, CreateDate [order by Database]
    out.add(new JdbcSelectTask("nz.v_database.csv", "SELECT * FROM system.._v_database"));

    // objid, DataType, Owner, Description, Size [order by DataType]
    out.add(new JdbcSelectTask("nz.v_datatype.csv", "SELECT * FROM system.._v_datatype"));

    // objid, Operator, Owner, CreateDate, Description, oprname, oprleft, oprright, oprresult,
    // oprcode, oprkind [order by Operator]
    out.add(new JdbcSelectTask("nz.v_operator.csv", "SELECT * FROM system.._v_operator"));

    // Not documented.
    out.add(new JdbcSelectTask("nz.v_objects.csv", "SELECT * FROM system.._v_objects"));

    // TODO; these might be placed in a ParallelTaskGroup?`
    // these neeed to be filtered on WHERE = dbname, or else which DB md table will contain SYSTEM
    // data too
    for (String db : dbs) {
      // The benefit of having this reduces the amount of data in the zip file
      // However, Shevek prefers to have all the information always available for debugging.
      // Also, case-sensitivity concerns (NZ is not always uppercase; `select identifier_case;`)
      // suggests that this filter is hard to construct reliably.
      String whereClause = " where DATABASE = upper('" + db + "')";
      // https://www.ibm.com/support/knowledgecenter/en/SSULQD_7.2.1/com.ibm.nz.adm.doc/t_sysadm_enable_multiple_schema.html
      String schemaPrefix =
          db + ".."; // We don't know what the default schema is, but it should contain the views
      // we require.
      String filePrefix =
          db.toUpperCase(); // If we have databases with the same name but mismatched case, this
      // will break.

      // also _v_relation_column: all attributes of a relation, Constraints and other informations
      // Undocumented?
      // TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,DATA_TYPE,TYPE_NAME,COLUMN_SIZE,BUFFER_LENGTH,DECIMAL_DIGITS,NUM_PREC_RADIX,
      // NULLABLE,REMARKS,COLUMN_DEF,SQL_DATA_TYPE,SQL_DATETIME_SUB,CHAR_OCTET_LENGTH,ORDINAL_POSITION,IS_NULLABLE,OBJID,DATABASE,OBJDB,SCHEMA,SCHEMAID
      // This has to match NetezzaDatabaseLoader.
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "netezza.columns.csv"),
              "SELECT DATABASE, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, TYPE_NAME FROM "
                  + schemaPrefix
                  + "_v_sys_columns ORDER BY TABLE_NAME, OBJID"));

      // objid, Function, Owner, CreateDate, Description, Result, Arguments [order by Function]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_function.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_function"));
      // objid, IndexName, TableName, Owner, CreateDate [order by TableName, IndexName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_index.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_index"));
      // objid, procedure, owner, createdate, objtype, description, result, numargs, arguments,
      // proceduresignature, builtin, proceduresource, sproc, executedasowner [order by procedure]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_procedure.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_procedure"));
      // objid, ObjectName, Owner, CreateDate, ObjectType, attnum, attname,
      // format_type(attypid,attypmod), attnotnullA [order by ObjectType, attnum]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_relation_column.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_relation_column"));
      // objid, ObjectName, Owner, CreateDate, Objecttype, attnum, attname, and adsrc [order by
      // Objecttype, attnum]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_relation_column_def.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_relation_column_def"));
      // Don't know... Database owner, relation, constraint name, contype, conseq, attname, pk
      // database, pk owner, pk relation, pk conseq, pk att name, updt_type, del_type, match_type,
      // deferrable, deferred, constr_ord ?
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_relation_keydata.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_relation_keydata"));
      // OBJID,TABLENAME,OWNER,CREATEDATE,DISTSEQNO,DISTATTNUM,ATTNUM,ATTNAME,DATABASE,OBJDB,SCHEMA,SCHEMAID
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, DistMapFormat.ZIP_ENTRY_SUFFIX),
              "SELECT * FROM " + schemaPrefix + "_v_table_dist_map"));
      // objid, SeqName, Owner, CreateDate [order by SeqName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_sequence.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_sequence"));
      // objid, TableName, Owner, CreateDate [order by TableName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_table.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_table"));
      // objid, UserName, Owner, ValidUntil, and CreateDate [order by UserName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_user.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_user"));
      // objid, ViewName, Owner, CreateDate, relhasindex, relkind, relchecks, reltriggers,
      // relhasrules, relukeys, relfkeys, relhaspkey, relnatts [order by ViewName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, Views.ZIP_ENTRY_SUFFIX),
              "SELECT * FROM " + schemaPrefix + "_v_view"));

      // Not documented.
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_sys_columns.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_sys_columns"));
      // Not documented.
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_sys_database.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_sys_database"));
      // objid, SysTableName, and Owner [order by SysTableName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_sys_table.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_sys_table"));
      // objid, SysViewName, and Owner [order by SysViewName]
      out.add(
          new JdbcSelectTask(
              withPrefix(filePrefix, "nz.v_sys_view.csv"),
              "SELECT * FROM " + schemaPrefix + "_v_sys_view"));
    }
  }

  @Override
  public Handle open(ConnectorArguments arguments) throws Exception {
    String url = arguments.getUri();
    if (url == null) {
      String host = arguments.getHost("localhost");
      int port = arguments.getPort(OPT_PORT_DEFAULT);
      url = "jdbc:netezza://" + host + ":" + port + "/system";
      //  "TMODE=ANSI,CHARSET=UTF8";
    }

    Driver driver = newDriver(arguments.getDriverPaths(), "org.netezza.Driver");
    DataSource dataSource =
        new SimpleDriverDataSource(driver, url, arguments.getUser(), arguments.getPassword());
    return new JdbcHandle(dataSource);
  }

  private static String withPrefix(String filePrefix, String name) {
    String separator = name.startsWith("/") ? "" : "/";
    return filePrefix + separator + name;
  }
}
