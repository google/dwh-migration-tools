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
package com.google.edwmigration.dumper.application.dumper.connector;

import static org.junit.Assert.*;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.task.ArgumentsTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.VersionTask;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author shevek */
public abstract class AbstractConnectorTest {

  @SuppressWarnings("UnusedVariable")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectorTest.class);

  protected static enum SpecialTaskType {
    Version,
    Arguments,
    Format
  }

  private static String[] A(String... in) {
    return in;
  }

  @Nonnull
  protected static List<String[]> newStandardArguments(@Nonnull Connector connector) {
    if (connector instanceof LogsConnector)
      return Arrays.asList(ArrayUtils.EMPTY_STRING_ARRAY, A("--query-log-days", "2"));
    return Collections.singletonList(ArrayUtils.EMPTY_STRING_ARRAY);
  }

  protected void testConnectorDefaults(@Nonnull Connector connector) throws Exception {
    for (String[] args : newStandardArguments(connector)) {
      testConnector(connector, args);
    }
  }

  protected void testConnector(@Nonnull Connector connector, String... args) throws Exception {
    List<String> a = new ArrayList<>();
    a.addAll(Arrays.asList("--connector", connector.getName()));
    Collections.addAll(a, args);
    ConnectorArguments arguments = new ConnectorArguments(a.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
    List<Task<?>> out = new ArrayList<>();
    connector.addTasksTo(out, arguments);

    Set<SpecialTaskType> specialTasks = EnumSet.noneOf(SpecialTaskType.class);

    LOG.debug("Tasks are:");
    for (Task<?> task : out) {
      LOG.debug(String.valueOf(task));
      if (task instanceof VersionTask) specialTasks.add(SpecialTaskType.Version);
      else if (task instanceof ArgumentsTask) specialTasks.add(SpecialTaskType.Arguments);
      else if (task instanceof FormatTask) specialTasks.add(SpecialTaskType.Format);
    }

    LOG.debug("Special tasks discovered are " + specialTasks);
    assertFalse(
        specialTasks.contains(
            SpecialTaskType.Version)); // Added by MetadataDumper, not the connector.
    assertFalse(
        specialTasks.contains(
            SpecialTaskType.Arguments)); // Added by MetadataDumper, not the connector.
    // TODO: assertTrue(specialTasks.contains(SpecialTaskType.Format));
  }
}
