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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleMetadataConnector.SelectTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleMetadataConnectorTest {

  @Test
  public void addTasksTo_generatesMetadataSelectTasks() throws Exception {
    OracleMetadataConnector connector = new OracleMetadataConnector();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    ImmutableList<String> selectSqls =
        tasks.stream()
            .filter(task -> task instanceof SelectTask)
            .map(task -> ((SelectTask) task).getSql())
            .collect(toImmutableList());

    assertEquals(
        selectSqls,
        ImmutableList.of(
            "SELECT * FROM ALL_Arguments",
            "SELECT * FROM DBA_Arguments",
            "SELECT * FROM ALL_Catalog",
            "SELECT * FROM DBA_Catalog",
            "SELECT * FROM ALL_Constraints",
            "SELECT * FROM DBA_Constraints",
            "SELECT * FROM ALL_Indexes",
            "SELECT * FROM DBA_Indexes",
            "SELECT * FROM ALL_MViews",
            "SELECT * FROM DBA_MViews",
            "SELECT * FROM ALL_Operators",
            "SELECT * FROM DBA_Operators",
            "SELECT * FROM ALL_Part_key_columns",
            "SELECT * FROM DBA_Part_key_columns",
            "SELECT * FROM ALL_Plsql_Types",
            "SELECT * FROM DBA_Plsql_Types",
            "SELECT * FROM ALL_Procedures",
            "SELECT * FROM DBA_Procedures",
            "SELECT * FROM ALL_Tab_Columns",
            "SELECT * FROM DBA_Tab_Columns",
            "SELECT * FROM ALL_Tab_Partitions",
            "SELECT * FROM DBA_Tab_Partitions",
            "SELECT * FROM ALL_Tables",
            "SELECT * FROM DBA_Tables",
            "SELECT * FROM ALL_Types",
            "SELECT * FROM DBA_Types",
            "SELECT * FROM ALL_Views",
            "SELECT * FROM DBA_Views",
            "SELECT OWNER, OBJECT_NAME, DBMS_METADATA.GET_XML('FUNCTION', OBJECT_NAME, OWNER) FROM DBA_OBJECTS WHERE OBJECT_NAME = 'FUNCTION'",
            "SELECT OWNER, OBJECT_NAME, DBMS_METADATA.GET_XML('FUNCTION', OBJECT_NAME, OWNER) FROM ALL_OBJECTS WHERE OBJECT_NAME = 'FUNCTION'",
            "SELECT OWNER, TABLE_NAME, DBMS_METADATA.GET_XML('TABLE', TABLE_NAME, OWNER) FROM DBA_TABLES WHERE NESTED='NO' AND (IOT_TYPE IS NULL OR IOT_TYPE='IOT')",
            "SELECT OWNER, TABLE_NAME, DBMS_METADATA.GET_XML('TABLE', TABLE_NAME, OWNER) FROM ALL_TABLES WHERE NESTED='NO' AND (IOT_TYPE IS NULL OR IOT_TYPE='IOT')",
            "SELECT OWNER, VIEW_NAME, DBMS_METADATA.GET_XML('VIEW', VIEW_NAME, OWNER) FROM DBA_VIEWS",
            "SELECT OWNER, VIEW_NAME, DBMS_METADATA.GET_XML('VIEW', VIEW_NAME, OWNER) FROM ALL_VIEWS",
            "SELECT OWNER, INDEX_NAME, DBMS_METADATA.GET_XML('INDEX', INDEX_NAME, OWNER) FROM DBA_INDEXES",
            "SELECT OWNER, INDEX_NAME, DBMS_METADATA.GET_XML('INDEX', INDEX_NAME, OWNER) FROM ALL_INDEXES",
            "SELECT SEQUENCE_OWNER, SEQUENCE_NAME, DBMS_METADATA.GET_XML('SEQUENCE', SEQUENCE_NAME, SEQUENCE_OWNER) FROM DBA_SEQUENCES",
            "SELECT SEQUENCE_OWNER, SEQUENCE_NAME, DBMS_METADATA.GET_XML('SEQUENCE', SEQUENCE_NAME, SEQUENCE_OWNER) FROM ALL_SEQUENCES",
            "SELECT OWNER, TYPE_NAME, DBMS_METADATA.GET_XML('TYPE', TYPE_NAME, OWNER) FROM DBA_TYPES WHERE PREDEFINED='NO'",
            "SELECT OWNER, TYPE_NAME, DBMS_METADATA.GET_XML('TYPE', TYPE_NAME, OWNER) FROM ALL_TYPES WHERE PREDEFINED='NO'",
            "SELECT OWNER, SYNONYM_NAME, DBMS_METADATA.GET_XML('SYNONYM', SYNONYM_NAME, OWNER) FROM DBA_SYNONYMS",
            "SELECT OWNER, SYNONYM_NAME, DBMS_METADATA.GET_XML('SYNONYM', SYNONYM_NAME, OWNER) FROM ALL_SYNONYMS"));
  }

  @Test
  public void getConnectorScope_success() {
    OracleMetadataConnector connector = new OracleMetadataConnector();
    assertEquals(OracleConnectorScope.METADATA, connector.getConnectorScope());
  }
}
