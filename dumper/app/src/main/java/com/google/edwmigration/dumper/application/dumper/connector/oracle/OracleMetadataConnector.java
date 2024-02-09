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

import static java.util.stream.Collectors.joining;

import autovalue.shaded.com.google.common.collect.ImmutableList;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
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
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
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

  private static final Logger LOG = LoggerFactory.getLogger(OracleMetadataConnector.class);
  private static final int XML_BATCH_SIZE = 100;

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

  @AutoValue
  protected abstract static class XmlQueryEntry {
    public abstract String owner();

    public abstract String name();

    public static XmlQueryEntry create(String owner, String name) {
      return new AutoValue_OracleMetadataConnector_XmlQueryEntry(owner, name);
    }
  }

  private static class SelectXmlTask extends AbstractJdbcTask<Void> implements GroupTask<Void> {

    private final String xmlQuery = "SELECT DBMS_METADATA.GET_XML(?, ?, ?) FROM DUAL";
    private final String rowSql;
    private final String objectType;
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
        String targetPath, String rowSql, String objectType, int ownerIndex, int nameIndex) {
      super(targetPath);
      this.rowSql = Preconditions.checkNotNull(rowSql, "Row SQL was null.");
      this.objectType = Preconditions.checkNotNull(objectType, "XML Object type was null.");
      this.ownerIndex = ownerIndex;
      this.nameIndex = nameIndex;
    }

    public SelectXmlTask(String targetPath, String rowSql, String objectType) {
      this(targetPath, rowSql, objectType, 1, 2);
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

    private String buildXmlSql(List<XmlQueryEntry> querySet) {
      return querySet.stream()
          .map(
              queryObject ->
                  String.format(
                      "SELECT DBMS_METADATA.GET_XML('%s', '%s', '%s') FROM DUAL",
                      objectType, queryObject.name(), queryObject.owner()))
          .collect(joining(" UNION ALL "));
    }

    @Nonnull
    private ResultSetExtractor<Void> newResultSetExtractor(
        @Nonnull ByteSink sink, @Nonnull JdbcHandle handle) {
      return new ResultSetExtractor<Void>() {

        private final JdbcTemplate jdbcTemplate = handle.getJdbcTemplate();

        @CheckForNull
        private List<String> getXmlData(List<XmlQueryEntry> querySet) {
          if (querySet == null || querySet.isEmpty()) {
            return ImmutableList.of();
          }

          try {
            return jdbcTemplate.queryForList(buildXmlSql(querySet), String.class);
          } catch (Exception e) {
            LOG.debug("Failed to retrieve XML for " + objectType + ": " + e);
            return ImmutableList.of();
          }
        }

        @Override
        public Void extractData(ResultSet rs) throws SQLException, DataAccessException {
          CSVFormat format = newCsvFormat(rs);
          try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
              RecordProgressMonitor monitor = new RecordProgressMonitor(getName());
              CSVPrinter printer = format.print(writer)) {

            ImmutableList.Builder<XmlQueryEntry> querySetBuilder = ImmutableList.builder();
            while (rs.next()) {
              querySetBuilder.add(
                  XmlQueryEntry.create(rs.getString(ownerIndex), rs.getString(nameIndex)));
            }

            List<List<XmlQueryEntry>> querySetChunks =
                Lists.partition(querySetBuilder.build(), XML_BATCH_SIZE);
            for (List<XmlQueryEntry> querySet : querySetChunks) {
              List<String> xmlResults = getXmlData(querySet);
              for (int i = 0; i < xmlResults.size(); i++) {
                printer.print(querySet.get(i).owner());
                printer.print(querySet.get(i).name());
                printer.print(xmlResults.get(i));
                printer.println();
                monitor.count();
              }
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
    public String describeSourceData() {
      return createSourceDataDescriptionForQuery(xmlQuery);
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

    // This shows up in dry-run
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

  private static SelectTask newSelectXmlTask(
      @Nonnull String file,
      @Nonnull String table,
      String objectType,
      String ownerColumn,
      String nameColumn,
      @Nonnull String where) {
    return new SelectTask(
        file,
        String.format(
            "SELECT %s, %s, DBMS_METADATA.GET_XML('%s', %s, %s) FROM %s u %s",
            ownerColumn, nameColumn, objectType, nameColumn, ownerColumn, table, where));
  }

  private static void buildSelectXmlTask(
      List<? super Task<?>> out,
      String all_file,
      String all_table,
      String dba_file,
      String dba_table,
      String objectType,
      String ownerColumn,
      String nameColumn,
      String whereCond) {
    SelectTask dba_task =
        newSelectXmlTask(dba_file, dba_table, objectType, ownerColumn, nameColumn, whereCond);
    SelectTask all_task =
        newSelectXmlTask(all_file, all_table, objectType, ownerColumn, nameColumn, whereCond);
    addAtLeastOneOf(out, dba_task, all_task);
  }

  @CheckForNull
  private static String toInList(@CheckForNull List<String> owners) {
    return (owners == null || owners.isEmpty())
        ? null
        : String.format("('%s')", String.join("','", owners));
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
    String whereCondFunctionOwner =
        " WHERE OBJECT_NAME = 'FUNCTION'"
            + (ownerInList == null ? "" : " AND OWNER IN " + ownerInList);
    // Since version 11g Oracle has introduced deferred segment creation.
    // This results in DBMS_METADATA.GET_XML not found error for tables with no segment created.
    String whereCondTableSegmentCreated =
        " WHERE SEGMENT_CREATED='YES'"
            + (ownerInList == null ? "" : " AND OWNER IN " + ownerInList);

    /*
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
    */

    // out.add(new JdbcSelectTask(Functions.ZIP_ENTRY_NAME,
    // "SELECT DBMS_METADATA.GET_DDL('FUNCTION', OBJECT_NAME) FROM USER_PROCEDURES"));
    // Double check this one
    // String sqlQuery = "SELECT OWNER, OBJECT_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlFunctions.ZIP_ENTRY_NAME_DBA,
    //         sqlQuery + "DBA_OBJECTS" + whereCondFunctionOwner,
    //         "FUNCTION"),
    //     new SelectXmlTask(
    //         XmlFunctions.ZIP_ENTRY_NAME_ALL,
    //         sqlQuery + "ALL_OBJECTS" + whereCondFunctionOwner,
    //         "FUNCTION"));

    buildSelectXmlTask(
        out,
        XmlFunctions.ZIP_ENTRY_NAME_ALL,
        "ALL_OBJECTS",
        XmlFunctions.ZIP_ENTRY_NAME_DBA,
        "DBA_OBJECTS",
        "FUNCTION",
        "OWNER",
        "OBJECT_NAME",
        whereCondFunctionOwner);

    // sqlQuery = "SELECT OWNER, TABLE_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlTables.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_TABLES" + whereCondTableIot, "TABLE"),
    //     new SelectXmlTask(
    //         XmlTables.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_TABLES" + whereCondTableIot, "TABLE"));

    buildSelectXmlTask(
        out,
        XmlTables.ZIP_ENTRY_NAME_ALL,
        "ALL_TABLES",
        XmlTables.ZIP_ENTRY_NAME_DBA,
        "DBA_TABLES",
        "TABLE",
        "OWNER",
        "TABLE_NAME",
        whereCondTableSegmentCreated);

    // sqlQuery = "SELECT OWNER, VIEW_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlViews.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_VIEWS" + whereCondOwner, "VIEW"),
    //     new SelectXmlTask(
    //         XmlViews.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_VIEWS" + whereCondOwner, "VIEW"));

    buildSelectXmlTask(
        out,
        XmlViews.ZIP_ENTRY_NAME_ALL,
        "ALL_VIEWS",
        XmlViews.ZIP_ENTRY_NAME_DBA,
        "DBA_VIEWS",
        "VIEW",
        "OWNER",
        "VIEW_NAME",
        whereCondOwner);

    // sqlQuery = "SELECT OWNER, INDEX_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlIndexes.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_INDEXES" + whereCondOwner, "INDEX"),
    //     new SelectXmlTask(
    //         XmlIndexes.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_INDEXES" + whereCondOwner, "INDEX"));

    buildSelectXmlTask(
        out,
        XmlIndexes.ZIP_ENTRY_NAME_ALL,
        "ALL_INDEXES",
        XmlIndexes.ZIP_ENTRY_NAME_DBA,
        "DBA_INDEXES",
        "INDEX",
        "OWNER",
        "INDEX_NAME",
        whereCondOwner);

    // sqlQuery = "SELECT SEQUENCE_OWNER, SEQUENCE_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlSequences.ZIP_ENTRY_NAME_DBA,
    //         sqlQuery + "DBA_SEQUENCES" + whereCondSequenceOwner,
    //         "SEQUENCE"),
    //     new SelectXmlTask(
    //         XmlSequences.ZIP_ENTRY_NAME_ALL,
    //         sqlQuery + "ALL_SEQUENCES" + whereCondSequenceOwner,
    //         "SEQUENCE"));

    buildSelectXmlTask(
        out,
        XmlSequences.ZIP_ENTRY_NAME_ALL,
        "ALL_SEQUENCES",
        XmlSequences.ZIP_ENTRY_NAME_DBA,
        "DBA_SEQUENCES",
        "SEQUENCE",
        "SEQUENCE_OWNER",
        "SEQUENCE_NAME",
        whereCondSequenceOwner);

    // sqlQuery = "SELECT OWNER, TYPE_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlTypes.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_TYPES" + whereCondOwner, "TYPE"),
    //     new SelectXmlTask(
    //         XmlTypes.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_TYPES" + whereCondOwner, "TYPE"));

    buildSelectXmlTask(
        out,
        XmlTypes.ZIP_ENTRY_NAME_ALL,
        "ALL_TYPES",
        XmlTypes.ZIP_ENTRY_NAME_DBA,
        "DBA_TYPES",
        "TYPE",
        "OWNER",
        "TYPE_NAME",
        whereCondOwner);

    // sqlQuery = "SELECT OWNER, SYNONYM_NAME FROM ";
    // addAtLeastOneOf(
    //     out,
    //     new SelectXmlTask(
    //         XmlSynonyms.ZIP_ENTRY_NAME_DBA, sqlQuery + "DBA_SYNONYMS" + whereCondOwner,
    // "SYNONYM"),
    //     new SelectXmlTask(
    //         XmlSynonyms.ZIP_ENTRY_NAME_ALL, sqlQuery + "ALL_SYNONYMS" + whereCondOwner,
    // "SYNONYM"));

    buildSelectXmlTask(
        out,
        XmlSynonyms.ZIP_ENTRY_NAME_ALL,
        "ALL_SYNONYMS",
        XmlSynonyms.ZIP_ENTRY_NAME_DBA,
        "DBA_SYNONYMS",
        "SYNONYM",
        "OWNER",
        "SYNONYM_NAME",
        whereCondOwner);

    // Todo: procedures, database links, triggers, packages
  }
}
