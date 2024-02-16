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
import com.google.edwmigration.dumper.application.dumper.connector.oracle.OracleMetadataConnector.SelectXmlTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OracleMetadataConnectorTest {

  @Test
  public void addTasksTo_generatesSelectXmlTasks() throws Exception {
    OracleMetadataConnector connector = new OracleMetadataConnector();
    List<Task<?>> tasks = new ArrayList<>();

    // Act
    connector.addTasksTo(tasks, new ConnectorArguments("--connector", connector.getName()));

    // Assert
    ImmutableList<SelectXmlTask> xmlTasks =
        tasks.stream()
            .filter(task -> task instanceof SelectXmlTask)
            .map(task -> (SelectXmlTask) task)
            .collect(toImmutableList());
    ImmutableList<String> rowSqls =
        xmlTasks.stream().map(task -> task.rowSql).collect(toImmutableList());
    assertEquals(
        ImmutableList.of(
            "SELECT OWNER, OBJECT_NAME FROM ALL_OBJECTS WHERE OBJECT_NAME = 'FUNCTION'",
            "SELECT OWNER, TABLE_NAME FROM DBA_TABLES",
            "SELECT OWNER, TABLE_NAME FROM ALL_TABLES",
            "SELECT OWNER, VIEW_NAME FROM DBA_VIEWS",
            "SELECT OWNER, VIEW_NAME FROM ALL_VIEWS",
            "SELECT OWNER, INDEX_NAME FROM DBA_INDEXES",
            "SELECT OWNER, INDEX_NAME FROM ALL_INDEXES",
            "SELECT SEQUENCE_OWNER, SEQUENCE_NAME FROM DBA_SEQUENCES",
            "SELECT SEQUENCE_OWNER, SEQUENCE_NAME FROM ALL_SEQUENCES",
            "SELECT OWNER, TYPE_NAME FROM DBA_TYPES",
            "SELECT OWNER, TYPE_NAME FROM ALL_TYPES",
            "SELECT OWNER, SYNONYM_NAME FROM DBA_SYNONYMS",
            "SELECT OWNER, SYNONYM_NAME FROM ALL_SYNONYMS"),
        rowSqls);
    ImmutableList<String> xmlSqls =
        xmlTasks.stream().map(task -> task.xmlSql).collect(toImmutableList());
    assertEquals(
        ImmutableList.of(
            "SELECT DBMS_METADATA.GET_XML('FUNCTION', ?, ?) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('TABLE', ?, ?) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('TABLE', ?, ?) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('VIEW', ?, ?) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('VIEW', ?, ?) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('INDEX', INDEX_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('INDEX', INDEX_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('SEQUENCE', SEQUENCE_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('SEQUENCE', SEQUENCE_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('TYPE', TYPE_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('TYPE', TYPE_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('SYNONYM', SYNONYM_NAME) FROM DUAL",
            "SELECT DBMS_METADATA.GET_XML('SYNONYM', SYNONYM_NAME) FROM DUAL"),
        xmlSqls);
  }
}
