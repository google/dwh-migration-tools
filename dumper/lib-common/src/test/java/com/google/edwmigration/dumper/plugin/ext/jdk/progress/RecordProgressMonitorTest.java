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
package com.google.edwmigration.dumper.plugin.ext.jdk.progress;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class RecordProgressMonitorTest {

  @Test
  public void count_totalExceeded_noExceptionThrown() {
    int counterTotal = 187317;
    // TODO: legacy value, (counterTotal + 1) should be enough for this test
    int updateCount = 2 + 2 * counterTotal;
    try (RecordProgressMonitor monitor = new RecordProgressMonitor("fast", counterTotal)) {
      // deliberate overflow
      for (int i = 0; i < updateCount; i++) {
        monitor.count();
      }
      assertEquals(updateCount, monitor.getCount());
    }
  }
}
