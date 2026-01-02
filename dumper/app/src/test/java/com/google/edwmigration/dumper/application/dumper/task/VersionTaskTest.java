/*
 * Copyright 2022-2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class VersionTaskTest {

  @Test
  public void testTask() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    TaskRunContext mockContext = mock(TaskRunContext.class);
    new VersionTask().doRun(mockContext, sink, () -> {});
  }

  @Test
  public void toString_success() {
    assertEquals(
        "Write compilerworks-version.txt from product version information.",
        new VersionTask().toString());
  }
}
