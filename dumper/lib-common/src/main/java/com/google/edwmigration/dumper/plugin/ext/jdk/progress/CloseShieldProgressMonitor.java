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

import java.util.concurrent.TimeUnit;

/** @author shevek */
public class CloseShieldProgressMonitor extends AbstractProgressMonitor {

  private final ProgressMonitor monitor;

  public CloseShieldProgressMonitor(ProgressMonitor monitor) {
    this.monitor = monitor;
  }

  @Override
  public long timeElapsed(TimeUnit desiredUnit) {
    return monitor.timeElapsed(desiredUnit);
  }

  @Override
  public long getCount() {
    return monitor.getCount();
  }

  @Override
  public long count(int delta) {
    return monitor.count(delta);
  }

  @Override
  public void close() {
    // Deliberate noop.
  }
}
