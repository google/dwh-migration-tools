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
package com.google.edwmigration.dumper.application.dumper.connector.cloudera;

import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import com.google.edwmigration.dumper.application.dumper.connector.hadoop.ScriptTmpDirCleanup;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.test.DummyMetaHandle;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContextFactory;
import com.google.edwmigration.dumper.application.dumper.test.InMemoryOutputHandle;
import com.google.edwmigration.dumper.application.dumper.test.InMemoryOutputHandleFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClouderaMetaConnectorTest extends AbstractConnectorTest {

  @BeforeClass
  public static void setUp() {
    ScriptTmpDirCleanup.cleanupAfterAllTestsAreFinished();
  }

  @Test
  public void testConnector() throws Exception {
    ClouderaConnector connector = new ClouderaConnector();

    ConnectorArguments arguments = new ConnectorArguments("--connector", connector.getName());
    List<Task<?>> out = new ArrayList<>();
    connector.addTasksTo(out, arguments);
    List<Task<?>> formatTasks =
        out.stream()
            .filter(t -> t.getName().equals("compilerworks-format.txt"))
            .collect(Collectors.toList());

    Handle handle = new DummyMetaHandle(arguments);
    InMemoryOutputHandleFactory outputHandleFactory = new InMemoryOutputHandleFactory();
    TaskRunContext context =
        DummyTaskRunContextFactory.create(outputHandleFactory, handle, arguments);
    for (Task<?> formatTask : formatTasks) {
      formatTask.run(context);
    }

    // Let's assert literal strings for the sake of being explicit.
    Map<String, InMemoryOutputHandle> handles = outputHandleFactory.getHandles();
    Assert.assertEquals(4, handles.size());
    Assert.assertEquals(
        handles.keySet(),
        ImmutableSet.of(
            "compilerworks-format.txt",
            "hiveql/compilerworks-format.txt",
            "cloudera-metadata/compilerworks-format.txt",
            "hdfs/compilerworks-format.txt"));

    Assert.assertEquals(handles.get("compilerworks-format.txt").getContent(), "cloudera.zip");
    Assert.assertEquals(
        handles.get("hiveql/compilerworks-format.txt").getContent(), "hiveql.dump.zip");
    Assert.assertEquals(
        handles.get("cloudera-metadata/compilerworks-format.txt").getContent(), "hadoop.dump.zip");
    Assert.assertEquals(handles.get("hdfs/compilerworks-format.txt").getContent(), "hdfs.zip");
  }
}
