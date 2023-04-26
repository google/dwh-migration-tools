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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.handle.JdbcHandle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractJdbcTask;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Summary;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.OracleMetadataDumpFormat;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Oracle")
public class OracleMetadataConnector extends AbstractOracleConnector
    implements MetadataConnector, OracleMetadataDumpFormat {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(OracleMetadataConnector.class);

  public OracleMetadataConnector() {
    super("oracle");
  }

  private static interface GroupTask<T> extends Task<T> {

    @CheckForNull
    public Exception getException();
  }

  private static class SelectTask extends JdbcSelectTask implements GroupTask<Summary> {

    private Exception throwable;

    public SelectTask(@Nonnull String file, @Nonnull String selectQuery) {
      super(file, selectQuery);
    }

    @Override
    public boolean handleException(Exception e) {
      throwable = e;
      return true;
    }

    @Override
    public Exception getException() {
      return throwable;
    }
  }

  private static class SelectXmlTask extends AbstractJdbcTask<Void> implements GroupTask<Void> {

    private final String rowSql;
    private final String xmlSql;
    private final int ownerIndex;
    private final int nameIndex;
    private Exception throwable;

    /**
     * @param targetPath
     * @param rowSql The SQL used to obtain the (outer) data rows.
     * @param xmlSql The SQL used to obtain the (inner) XML result.
     * @param ownerIndex The index of the object owner name in the outer ResultSet.
     * @param nameIndex The index of the object name in the outer ResultSet.
     */
    public SelectXmlTask(
        String targetPath, String rowSql, String xmlSql, int ownerIndex, int nameIndex) {
      super(targetPath);
      this.rowSql = Preconditions.checkNotNull(xmlSql, "Row SQL was null.");
      this.xmlSql = Preconditions.checkNotNull(xmlSql, "XML SQL was null.");
      this.ownerIndex = ownerIndex;
      this.nameIndex = nameIndex;
    }

    public SelectXmlTask(String targetPath, String rowSql, String xmlSql) {
      this(targetPath, rowSql, xmlSql, 1, 2);
    }

    @Override
    public boolean handleException(Exception e) {
      throwable = e;
      return true;
    }

    @Override
    public Exception getException() {
      return throwable;
    }

    @Nonnull
    private ResultSetExtractor<Void> newResultSetExtractor(
        @Nonnull ByteSink sink, @Nonnull JdbcHandle handle) {
      return new ResultSetExtractor<Void>() {

        private final JdbcTemplate jdbcTemplate = handle.getJdbcTemplate();

        @CheckForNull
        private String getXmlData(@Nonnull String owner, @Nonnull String name) {
          try {
            return jdbcTemplate.queryForObject(xmlSql, String.class, name, owner);
          } catch (Exception e) {
            LOG.debug("Failed to retrieve XML for " + name + ": " + e);
            return null;
          }
        }

        @Override
        public Void extractData(ResultSet rs) throws SQLException, DataAccessException {
          CSVFormat format = newCsvFormat(rs);
          try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
              CSVPrinter printer = format.print(writer)) {
            final int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
              for (int i = 1; i <= columnCount; i++) printer.print(rs.getObject(i));
              String owner = rs.getString(ownerIndex);
              String name = rs.getString(nameIndex);
              printer.print(getXmlData(owner, name));
              printer.println();
            }
            return null;
          } catch (IOException e) {
            throw new SQLException(e);
          }
        }
      };
    }

    @Override
    protected Void doInConnection(
        @Nonnull TaskRunContext context,
        @Nonnull JdbcHandle jdbcHandle,
        @Nonnull ByteSink sink,
        @Nonnull Connection connection)
        throws SQLException {
      ResultSetExtractor<Void> rse = newResultSetExtractor(sink, jdbcHandle);
      return doSelect(connection, rse, rowSql);
    }

    @Override
    public String toString() {
      return super.toString() + "\n    " + xmlSql;
    }
  }

  private static class MessageTask extends AbstractTask<Void> {

    private final GroupTask[] tasks;

    public MessageTask(@Nonnull GroupTask... ts) {
      super(String.join(", ", Lists.transform(Arrays.asList(ts), GroupTask::getName)));
      tasks = ts;
    }

    // if we are here, means both the dep tasks *have* failed.
    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      int c = 1;
      LOG.error("All the select tasks failed:");
      for (GroupTask task : tasks) {
        LOG.error(
            "("
                + c
                + "): "
                + task.getName()
                + " : "
                + ExceptionUtils.getRootCauseMessage(task.getException()));
        c += 1;
      }
      return null;
    }

    // This shwos up in dry-run
    @Override
    public String toString() {
      return "[ Error if all fail: "
          + String.join(", ", Lists.transform(Arrays.asList(tasks), GroupTask::getName))
          + " ]";
    }
  }

  private static void addAtLeastOneOf(
      @Nonnull List<? super Task<?>> out, @Nonnull GroupTask<?>... tasks) {
    for (GroupTask<?> task : tasks) out.add(Preconditions.checkNotNull(task));
    MessageTask msg_task = new MessageTask(tasks);
    msg_task.onlyIfAllFailed(tasks);
    out.add(msg_task);
  }

  @Nonnull
  private static SelectTask newSelectStarTask(
      @Nonnull String file, @Nonnull String table, @Nonnull String where) {
    return new SelectTask(file, "SELECT * FROM " + table + where);
  }

  private static void buildSelectStarTask(
      List<? super Task<?>> out,
      String all_file,
      String all_table,
      String dba_file,
      String dba_table,
      @Nonnull String whereCond) {
    SelectTask dba_task = newSelectStarTask(dba_file, dba_table, whereCond);
    SelectTask all_task = newSelectStarTask(all_file, all_table, whereCond);
    addAtLeastOneOf(out, dba_task, all_task);
  }

  @CheckForNull
  private static String toInList(@CheckForNull List<String> owners) {
    if (owners == null || owners.isEmpty()) return null;
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String owner : owners) {
      if (first) {
        first = false;
        sb.append("('").append(owner).append("'");
      } else {
        sb.append(",'").append(owner).append("'");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(OracleMetadataDumpFormat.FORMAT_NAME));

    out.add(new JdbcSelectTask(V_Version.ZIP_ENTRY_NAME, "SELECT * from V$VERSION"));
    out.add(new JdbcSelectTask(V_Parameter2.ZIP_ENTRY_NAME, "SELECT * from V$PARAMETER2"));
    out.add(new JdbcSelectTask(V_Pwfile_users.ZIP_ENTRY_NAME, "SELECT * from V$PWFILE_USERS"));
    out.add(new JdbcSelectTask(V_Option.ZIP_ENTRY_NAME, "SELECT * from V$OPTION"));

    String ownerInList = toInList(arguments.getSchemata());
    String whereCondOwner = ownerInList == null ? "" : " WHERE OWNER IN " + ownerInList;
    String whereCondTableOwner = ownerInList == null ? "" : " WHERE TABLE_OWNER IN " + ownerInList;
    String whereCondSequenceOwner =
        ownerInList == null ? "" : " WHERE SEQUENCE_OWNER IN " + ownerInList;

    buildSelectStarTask(
        out,
        Arguments.ZIP_ENTRY_NAME_DBA,
        "DBA_Arguments",
        Arguments.ZIP_ENTRY_NAME_ALL,
        "All_Arguments",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Catalog.ZIP_ENTRY_NAME_DBA,
        "DBA_Catalog",
        Catalog.ZIP_ENTRY_NAME_ALL,
        "All_Catalog",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Constraints.ZIP_ENTRY_NAME_DBA,
        "DBA_Constraints",
        Constraints.ZIP_ENTRY_NAME_ALL,
        "All_Constraints",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Indexes.ZIP_ENTRY_NAME_DBA,
        "DBA_Indexes",
        Indexes.ZIP_ENTRY_NAME_ALL,
        "All_Indexes",
        whereCondOwner);
    buildSelectStarTask(
        out,
        MViews.ZIP_ENTRY_NAME_DBA,
        "DBA_MViews",
        MViews.ZIP_ENTRY_NAME_ALL,
        "All_MViews",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Operators.ZIP_ENTRY_NAME_DBA,
        "DBA_Operators",
        Operators.ZIP_ENTRY_NAME_ALL,
        "All_Operators",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Part_key_columns.ZIP_ENTRY_NAME_DBA,
        "DBA_Part_key_columns",
        Part_key_columns.ZIP_ENTRY_NAME_ALL,
        "All_Part_key_columns",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Plsql_Types.ZIP_ENTRY_NAME_DBA,
        "DBA_Plsql_Types",
        Plsql_Types.ZIP_ENTRY_NAME_ALL,
        "All_Plsql_Types",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Procedures.ZIP_ENTRY_NAME_DBA,
        "DBA_Procedures",
        Procedures.ZIP_ENTRY_NAME_ALL,
        "All_Procedures",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Tab_Columns.ZIP_ENTRY_NAME_DBA,
        "DBA_Tab_Columns",
        Tab_Columns.ZIP_ENTRY_NAME_ALL,
        "All_Tab_Columns",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Tab_Partitions.ZIP_ENTRY_NAME_DBA,
        "DBA_Tab_Partitions",
        Tab_Partitions.ZIP_ENTRY_NAME_ALL,
        "All_Tab_Partitions",
        whereCondTableOwner);
    buildSelectStarTask(
        out,
        Tables.ZIP_ENTRY_NAME_DBA,
        "DBA_Tables",
        Tables.ZIP_ENTRY_NAME_ALL,
        "All_Tables",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Types.ZIP_ENTRY_NAME_DBA,
        "DBA_Types",
        Types.ZIP_ENTRY_NAME_ALL,
        "All_Types",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Views.ZIP_ENTRY_NAME_DBA,
        "DBA_Views",
        Views.ZIP_ENTRY_NAME_ALL,
        "All_Views",
        whereCondOwner);

    String sqlQuery, xmlQuery;

    // out.add(new JdbcSelectTask(Functions.ZIP_ENTRY_NAME,
    // "SELECT DBMS_METADATA.GET_DDL('FUNCTION', OBJECT_NAME) FROM USER_PROCEDURES"));
    // Double check this one
    String whereCondFunctionOwner = ownerInList == null ? "" : " AND OWNER IN " + ownerInList;
    out.add(
        new SelectXmlTask(
            XmlFunctions.ZIP_ENTRY_NAME,
            "SELECT OWNER, OBJECT_NAME FROM ALL_OBJECTS WHERE OBJECT_NAME = 'FUNCTION'"
                + whereCondFunctionOwner,
            "SELECT DBMS_METADATA.GET_XML('FUNCTION', ?, ?) FROM DUAL"));

    sqlQuery = "SELECT OWNER, TABLE_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('TABLE', ?, ?) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlTables.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_TABLES" + whereCondOwner, xmlQuery),
        new SelectXmlTask(
            XmlTables.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_TABLES" + whereCondOwner, xmlQuery));

    sqlQuery = "SELECT OWNER, VIEW_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('VIEW', ?, ?) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlViews.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_VIEWS" + whereCondOwner, xmlQuery),
        new SelectXmlTask(
            XmlViews.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_VIEWS" + whereCondOwner, xmlQuery));

    sqlQuery = "SELECT OWNER, INDEX_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('INDEX', INDEX_NAME) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlIndexes.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_INDEXES" + whereCondOwner, xmlQuery),
        new SelectXmlTask(
            XmlIndexes.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_INDEXES" + whereCondOwner, xmlQuery));

    sqlQuery = "SELECT SEQUENCE_OWNER, SEQUENCE_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('SEQUENCE', SEQUENCE_NAME) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlSequences.ZIP_ENTRY_NAME_DBA,
            sqlQuery + "DBA_SEQUENCES" + whereCondSequenceOwner,
            xmlQuery),
        new SelectXmlTask(
            XmlSequences.ZIP_ENTRY_NAME_ALL,
            sqlQuery + "ALL_SEQUENCES" + whereCondSequenceOwner,
            xmlQuery));

    sqlQuery = "SELECT OWNER, TYPE_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('TYPE', TYPE_NAME) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlTypes.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_TYPES" + whereCondOwner, xmlQuery),
        new SelectXmlTask(
            XmlTypes.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_TYPES" + whereCondOwner, xmlQuery));

    sqlQuery = "SELECT OWNER, SYNONYM_NAME FROM ";
    xmlQuery = "SELECT DBMS_METADATA.GET_XML('SYNONYM', SYNONYM_NAME) FROM DUAL";
    addAtLeastOneOf(
        out,
        new SelectXmlTask(
            XmlSynonyms.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_SYNONYMS" + whereCondOwner, xmlQuery),
        new SelectXmlTask(
            XmlSynonyms.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_SYNONYMS" + whereCondOwner, xmlQuery));
    // Todo: procedures, database links, triggers, packages
  }
}
