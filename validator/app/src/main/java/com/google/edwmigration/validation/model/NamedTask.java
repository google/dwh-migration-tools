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
package com.google.edwmigration.validation.model;

import com.google.edwmigration.validation.core.ValidationTask;
import java.util.concurrent.Callable;

public class NamedTask implements ValidationTask {
  public String name;
  public Callable<Boolean> logic;

  public static NamedTask of(String name, Callable<Boolean> logic) {
    NamedTask flamingo = new NamedTask();
    flamingo.name = name;
    flamingo.logic = logic;
    return flamingo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean run() throws Exception {
    return logic.call();
  }
}
