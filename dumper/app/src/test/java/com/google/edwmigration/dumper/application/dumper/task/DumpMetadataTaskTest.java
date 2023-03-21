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
package com.google.edwmigration.dumper.application.dumper.task;

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.test.DummyTaskRunContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class DumpMetadataTaskTest extends AbstractTaskTest {

  @Test
  public void testTask() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    ConnectorArguments arguments =
        new ConnectorArguments(new String[] {"--connector", "bigquery-logs"});
    new DumpMetadataTask(arguments, "test-format")
        .doRun(new DummyTaskRunContext(HANDLE), sink, HANDLE);
  }
}
