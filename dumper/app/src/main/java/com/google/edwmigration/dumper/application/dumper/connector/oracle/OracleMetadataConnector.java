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
package com.google.edwmigration.dumper.application.dumper.connector.oracle;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.task.GroupTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.JdbcSelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.OracleMetadataDumpFormat;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Oracle")
public class OracleMetadataConnector extends AbstractOracleConnector
    implements MetadataConnector, OracleMetadataDumpFormat {

  private static final Logger LOG = LoggerFactory.getLogger(OracleMetadataConnector.class);

  public OracleMetadataConnector() {
    super(OracleConnectorScope.METADATA);
  }

  private static void addAtLeastOneOf(
      @Nonnull List<? super Task<?>> out, @Nonnull GroupTask... tasks) {
    for (GroupTask task : tasks) out.add(Preconditions.checkNotNull(task));
    Task<?> msg_task = MessageTask.create(tasks).onlyIfAllFailed(tasks);
    out.add(msg_task);
  }

  @Nonnull
  private static GroupTask newSelectStarTask(
      @Nonnull String file, @Nonnull String table, @Nonnull String where) {
    checkArgument(table.endsWith(" ") || where.startsWith(" ") || where.isEmpty());
    return GroupTask.createSelect(file, "SELECT * FROM " + table + where);
  }

  private static void buildSelectStarTask(
      @Nonnull List<? super Task<?>> out,
      @Nonnull String dba_file,
      @Nonnull String dba_table,
      @Nonnull String all_file,
      @Nonnull String all_table,
      @Nonnull String whereCond) {
    GroupTask all_task = newSelectStarTask(all_file, all_table, whereCond);
    GroupTask dba_task = newSelectStarTask(dba_file, dba_table, whereCond);
    addAtLeastOneOf(out, all_task, dba_task);
  }

  private static GroupTask newSelectXmlTask(
      @Nonnull String file,
      @Nonnull String table,
      @Nonnull String objectType,
      @Nonnull String ownerColumn,
      @Nonnull String nameColumn,
      @Nonnull String where) {
    return GroupTask.createSelect(
        file,
        String.format(
            "SELECT %s, %s, DBMS_METADATA.GET_XML('%s', %s, %s) FROM %s%s",
            ownerColumn, nameColumn, objectType, nameColumn, ownerColumn, table, where));
  }

  private static void buildSelectXmlTask(
      @Nonnull List<? super Task<?>> out,
      @Nonnull String all_file,
      @Nonnull String all_table,
      @Nonnull String dba_file,
      @Nonnull String dba_table,
      @Nonnull String objectType,
      @Nonnull String ownerColumn,
      @Nonnull String nameColumn,
      @Nonnull String whereCond) {
    GroupTask dba_task =
        newSelectXmlTask(dba_file, dba_table, objectType, ownerColumn, nameColumn, whereCond);
    GroupTask all_task =
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
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws Exception {
    out.add(new DumpMetadataTask(arguments, getFormatName()));
    out.add(new FormatTask(getFormatName()));

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
    // XML metadata does not exist for iot overflow and nested tables what causes `not found`
    // exception.
    String whereCondTableXmlMetadata =
        " WHERE NESTED='NO' AND (IOT_TYPE IS NULL OR IOT_TYPE='IOT')"
            + (ownerInList == null ? "" : " AND OWNER IN " + ownerInList);
    // XML metadata does not exist for predefined oracle types what result in `not found` exception.
    String whereCondTypesNoPredefined =
        " WHERE PREDEFINED='NO'" + (ownerInList == null ? "" : " AND OWNER IN " + ownerInList);

    buildSelectStarTask(
        out,
        Arguments.ZIP_ENTRY_NAME_DBA,
        "DBA_Arguments",
        Arguments.ZIP_ENTRY_NAME_ALL,
        "ALL_Arguments",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Catalog.ZIP_ENTRY_NAME_DBA,
        "DBA_Catalog",
        Catalog.ZIP_ENTRY_NAME_ALL,
        "ALL_Catalog",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Constraints.ZIP_ENTRY_NAME_DBA,
        "DBA_Constraints",
        Constraints.ZIP_ENTRY_NAME_ALL,
        "ALL_Constraints",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Indexes.ZIP_ENTRY_NAME_DBA,
        "DBA_Indexes",
        Indexes.ZIP_ENTRY_NAME_ALL,
        "ALL_Indexes",
        whereCondOwner);
    buildSelectStarTask(
        out,
        MViews.ZIP_ENTRY_NAME_DBA,
        "DBA_MViews",
        MViews.ZIP_ENTRY_NAME_ALL,
        "ALL_MViews",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Operators.ZIP_ENTRY_NAME_DBA,
        "DBA_Operators",
        Operators.ZIP_ENTRY_NAME_ALL,
        "ALL_Operators",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Part_key_columns.ZIP_ENTRY_NAME_DBA,
        "DBA_Part_key_columns",
        Part_key_columns.ZIP_ENTRY_NAME_ALL,
        "ALL_Part_key_columns",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Plsql_Types.ZIP_ENTRY_NAME_DBA,
        "DBA_Plsql_Types",
        Plsql_Types.ZIP_ENTRY_NAME_ALL,
        "ALL_Plsql_Types",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Procedures.ZIP_ENTRY_NAME_DBA,
        "DBA_Procedures",
        Procedures.ZIP_ENTRY_NAME_ALL,
        "ALL_Procedures",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Sequences.ZIP_ENTRY_NAME_DBA,
        "DBA_Sequences",
        Sequences.ZIP_ENTRY_NAME_ALL,
        "ALL_Sequences",
        whereCondSequenceOwner);
    buildSelectStarTask(
        out,
        Tab_Columns.ZIP_ENTRY_NAME_DBA,
        "DBA_Tab_Columns",
        Tab_Columns.ZIP_ENTRY_NAME_ALL,
        "ALL_Tab_Columns",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Tab_Partitions.ZIP_ENTRY_NAME_DBA,
        "DBA_Tab_Partitions",
        Tab_Partitions.ZIP_ENTRY_NAME_ALL,
        "ALL_Tab_Partitions",
        whereCondTableOwner);
    buildSelectStarTask(
        out,
        Tables.ZIP_ENTRY_NAME_DBA,
        "DBA_Tables",
        Tables.ZIP_ENTRY_NAME_ALL,
        "ALL_Tables",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Types.ZIP_ENTRY_NAME_DBA,
        "DBA_Types",
        Types.ZIP_ENTRY_NAME_ALL,
        "ALL_Types",
        whereCondOwner);
    buildSelectStarTask(
        out,
        Views.ZIP_ENTRY_NAME_DBA,
        "DBA_Views",
        Views.ZIP_ENTRY_NAME_ALL,
        "ALL_Views",
        whereCondOwner);

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

    buildSelectXmlTask(
        out,
        XmlTables.ZIP_ENTRY_NAME_ALL,
        "ALL_TABLES",
        XmlTables.ZIP_ENTRY_NAME_DBA,
        "DBA_TABLES",
        "TABLE",
        "OWNER",
        "TABLE_NAME",
        whereCondTableXmlMetadata);

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

    buildSelectXmlTask(
        out,
        XmlTypes.ZIP_ENTRY_NAME_ALL,
        "ALL_TYPES",
        XmlTypes.ZIP_ENTRY_NAME_DBA,
        "DBA_TYPES",
        "TYPE",
        "OWNER",
        "TYPE_NAME",
        whereCondTypesNoPredefined);

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
