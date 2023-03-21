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
package com.google.edwmigration.dumper.plugin.ext.jdk.progress;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** @author shevek */
@RunWith(JUnit4.class)
public class RecordProgressMonitorTest {

  @Test
  public void testFast() {
    int n = 187317;
    RecordProgressMonitor monitor = new RecordProgressMonitor("fast", n);
    for (int i = 0; i < n * 2; i++) // deliberate overflow
    monitor.count();
    monitor.count();
    monitor.count();
  }

  @Ignore("ohmygodslow")
  @Test
  public void testSlow() throws Exception {
    int n = 1827317;
    RecordProgressMonitor monitor = new RecordProgressMonitor("fast", n);
    for (int i = 0; i < n; i++) {
      Thread.sleep(100);
      monitor.count();
    }
    monitor.count();
    monitor.count();
  }
}
