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

import com.google.edwmigration.dumper.application.dumper.metrics.*;

/**
 * Strategy interface for handling telemetry operations. This replaces the boolean shouldWrite flag
 * with a more flexible approach.
 */
public interface TelemetryWriteStrategy {
  /**
   * Processes individual telemetry data point for immediate streaming.
   *
   * @param telemetryEvent the telemetry object to process
   */
  void process(TelemetryEvent telemetryEvent);

  /** Flushes individual telemetry data point immediately */
  void flush();
}
