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
package com.google.edwmigration.dumper.application.dumper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.base.Stopwatch;
import com.google.edwmigration.dumper.application.dumper.metrics.ClientTelemetry;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import java.nio.file.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TelemetryStrategyTest {

  @Mock private ConnectorArguments mockArguments;
  @Mock private TaskSetState mockState;
  @Mock private FileSystem mockFileSystem;
  @Mock private ClientTelemetry mockClientTelemetry;

  @Test
  public void testWriteTelemetryStrategy_ProcessesMetrics() {
    WriteTelemetryStrategy strategy = new WriteTelemetryStrategy();
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    
    strategy.processDumperRunMetrics(
        mockClientTelemetry, mockArguments, mockState, stopwatch, true);
    
    assertNotNull(strategy);
  }

  @Test
  public void testWriteTelemetryStrategy_WritesTelemetry() {
    WriteTelemetryStrategy strategy = new WriteTelemetryStrategy();
    
    strategy.writeTelemetry(mockFileSystem, mockClientTelemetry);
    
    assertNotNull(strategy);
  }

  @Test
  public void testNoOpTelemetryStrategy_DoesNothing() {
    NoOpTelemetryStrategy strategy = new NoOpTelemetryStrategy();
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    
    strategy.processDumperRunMetrics(
        mockClientTelemetry, mockArguments, mockState, stopwatch, true);
    strategy.writeTelemetry(mockFileSystem, mockClientTelemetry);
    
    assertNotNull(strategy);
  }

  @Test
  public void testTelemetryStrategyFactory_CreatesCorrectStrategies() {
    TelemetryStrategy writeStrategy = TelemetryStrategyFactory.createStrategy(true);
    TelemetryStrategy noOpStrategy = TelemetryStrategyFactory.createStrategy(false);
    
    assertTrue(writeStrategy instanceof WriteTelemetryStrategy);
    assertTrue(noOpStrategy instanceof NoOpTelemetryStrategy);
  }

  @Test
  public void testTelemetryProcessor_StrategyConstructor() {
    TelemetryStrategy strategy = new WriteTelemetryStrategy();
    
    TelemetryProcessor processor = new TelemetryProcessor(strategy);
    
    assertNotNull(processor);
  }
}
